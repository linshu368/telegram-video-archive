-- Telegram-thumbnail covers: an independent two-stage track beside the video's own.
-- Apply as database owner on an existing drama_archive schema (pipeline.py migrate
-- --migration 004_cover.sql). Only columns, the stage domain and three function
-- bodies change; table set, function set, owners and grants are untouched, so
-- Database.verify_worker()'s contract still holds.
BEGIN;
ALTER TABLE drama_archive.videos
    ADD COLUMN cover_path text UNIQUE
        CHECK (cover_path = 'covers/' || id::text || '/original.jpg'),
    -- Terminal explanation for a video that will never have a cover (currently only
    -- 'no_thumbnail_available'): cover_path IS NULL alone cannot tell "missing" from "none exists".
    ADD COLUMN cover_note text CHECK (cover_note ~ '^[a-z0-9_]{1,80}$');
-- Finding the still-uncovered videos must not scan the whole table.
CREATE INDEX videos_missing_cover ON drama_archive.videos(id)
    WHERE cover_path IS NULL AND cover_note IS NULL;

ALTER TABLE drama_archive.pipeline_tasks DROP CONSTRAINT pipeline_tasks_stage_check;
ALTER TABLE drama_archive.pipeline_tasks ADD CONSTRAINT pipeline_tasks_stage_check
    CHECK (stage IN ('download', 'upload', 'ai', 'cover_download', 'cover_upload'));

-- Replaces only the stage list: every call (including for already-imported videos)
-- backfills the two cover task rows through the existing ON CONFLICT DO NOTHING.
CREATE OR REPLACE FUNCTION drama_archive.import_video(d jsonb, v jsonb) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
DECLARE existing drama_archive.videos; did uuid; vid uuid;
BEGIN
    did := (d->>'id')::uuid; vid := (v->>'id')::uuid;
    IF (v->>'drama_id')::uuid IS DISTINCT FROM did THEN
        RAISE EXCEPTION 'drama_identity_conflict';
    END IF;
    INSERT INTO drama_archive.dramas(id,title,source_id,topic_id)
      VALUES (did,d->>'title',(d->>'source_id')::bigint,(d->>'topic_id')::bigint)
      ON CONFLICT DO NOTHING;  -- any unique key: concurrent imports of one drama race on id and (source_id,topic_id)
    IF NOT EXISTS (SELECT FROM drama_archive.dramas WHERE id=did
        AND source_id=(d->>'source_id')::bigint AND topic_id=(d->>'topic_id')::bigint) THEN
        RAISE EXCEPTION 'drama_identity_conflict';
    END IF;
    INSERT INTO drama_archive.videos(id,drama_id,source_id,message_id,media_id,
        original_title,original_tags,original_description,episode)
      VALUES (vid,did,(v->>'source_id')::bigint,(v->>'message_id')::bigint,
        (v->>'media_id')::bigint,v->>'original_title',
        ARRAY(SELECT jsonb_array_elements_text(v->'original_tags')),
        v->>'original_description',v->>'episode')
      ON CONFLICT (source_id,message_id) DO NOTHING;
    SELECT * INTO existing FROM drama_archive.videos
      WHERE source_id=(v->>'source_id')::bigint AND message_id=(v->>'message_id')::bigint;
    IF existing.id <> vid OR existing.drama_id <> did OR existing.media_id <> (v->>'media_id')::bigint THEN
        RAISE EXCEPTION 'video_identity_conflict';
    END IF;
    INSERT INTO drama_archive.pipeline_tasks(video_id,stage)
      SELECT vid, unnest(ARRAY['download','upload','ai','cover_download','cover_upload'])
      ON CONFLICT DO NOTHING;
    RETURN vid;
END $$;

-- Cover stages depend on each other and on nothing else: a failed or unfinished
-- video download/upload must not stop a cover, nor a missing cover a video.
CREATE OR REPLACE FUNCTION drama_archive.claim(vid uuid, step text, seconds integer, max_attempts integer, backoff integer)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
DECLARE token uuid := gen_random_uuid(); result uuid;
BEGIN
    IF seconds NOT BETWEEN 5 AND 3600 OR max_attempts < 1 OR backoff < 0 THEN
        RAISE EXCEPTION 'invalid_lease_configuration';
    END IF;
    UPDATE drama_archive.pipeline_tasks t SET status='running', attempts=attempts+1,
        lease_token=token, lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
        updated_at=clock_timestamp(), last_error=NULL
    WHERE t.video_id=vid AND t.stage=step AND t.attempts<max_attempts AND
      (t.status='pending' OR (t.status='failed' AND t.updated_at <= clock_timestamp()-make_interval(secs=>backoff))
       OR (t.status='running' AND t.lease_expires_at <= clock_timestamp()))
      AND ((step='download' AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND object_path IS NULL))
        OR (step='upload' AND EXISTS (SELECT FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage='download' AND status='succeeded'))
        OR (step='ai' AND EXISTS (SELECT FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage='upload' AND status='succeeded')
             AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND object_path IS NOT NULL))
        OR (step='cover_download' AND EXISTS (SELECT FROM drama_archive.videos
             WHERE id=vid AND cover_path IS NULL AND cover_note IS NULL))
        OR (step='cover_upload' AND EXISTS (SELECT FROM drama_archive.pipeline_tasks
             WHERE video_id=vid AND stage='cover_download' AND status='succeeded')
             AND EXISTS (SELECT FROM drama_archive.videos
             WHERE id=vid AND cover_path IS NULL AND cover_note IS NULL)))
    RETURNING lease_token INTO result;
    RETURN result;
END $$;

CREATE OR REPLACE FUNCTION drama_archive.finish(vid uuid, step text, token uuid, payload jsonb DEFAULT '{}') RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
DECLARE task drama_archive.pipeline_tasks;
BEGIN
    SELECT * INTO task FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage=step FOR UPDATE;
    IF task.status IS DISTINCT FROM 'running' OR task.lease_token IS DISTINCT FROM token
        OR task.lease_expires_at <= clock_timestamp() THEN RAISE EXCEPTION 'lease_lost'; END IF;
    IF step='upload' THEN
        IF payload->>'object_path' IS DISTINCT FROM 'videos/' || vid::text || '/original' THEN
            RAISE EXCEPTION 'object_identity_conflict';
        END IF;
        UPDATE drama_archive.videos SET object_path=payload->>'object_path' WHERE id=vid;
        IF NOT EXISTS (SELECT FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage='ai') THEN
            RAISE EXCEPTION 'ai_task_missing';
        END IF;
    ELSIF step='cover_download' THEN
        -- A note means Telegram itself has no fetchable thumbnail. Nothing can be
        -- uploaded, so cover_upload is closed out here rather than left pending.
        IF payload->>'cover_note' IS NOT NULL THEN
            UPDATE drama_archive.videos SET cover_note=payload->>'cover_note'
              WHERE id=vid AND cover_path IS NULL;
            UPDATE drama_archive.pipeline_tasks SET status='succeeded',lease_token=NULL,
              lease_expires_at=NULL,last_error=NULL,updated_at=clock_timestamp()
              WHERE video_id=vid AND stage='cover_upload' AND status <> 'succeeded';
        END IF;
    ELSIF step='cover_upload' THEN
        IF payload->>'cover_path' IS DISTINCT FROM 'covers/' || vid::text || '/original.jpg' THEN
            RAISE EXCEPTION 'cover_identity_conflict';
        END IF;
        -- A real cover supersedes any earlier "no thumbnail" conclusion.
        UPDATE drama_archive.videos SET cover_path=payload->>'cover_path', cover_note=NULL WHERE id=vid;
    ELSIF step='ai' THEN
        IF jsonb_typeof(payload->'tags') IS DISTINCT FROM 'array'
           OR EXISTS (SELECT FROM jsonb_array_elements(payload->'tags') x WHERE jsonb_typeof(x) <> 'string')
           OR (payload->'title' IS NOT NULL AND jsonb_typeof(payload->'title') NOT IN ('string','null'))
           OR (payload->'description' IS NOT NULL AND jsonb_typeof(payload->'description') NOT IN ('string','null')) THEN
            RAISE EXCEPTION 'invalid_ai_output';
        END IF;
        INSERT INTO drama_archive.ai_results(video_id,title,tags,description)
          VALUES (vid,NULLIF(btrim(payload->>'title'),''),ARRAY(SELECT DISTINCT btrim(x)
            FROM jsonb_array_elements_text(payload->'tags') x WHERE btrim(x)<>''),NULLIF(btrim(payload->>'description'),''))
          ON CONFLICT (video_id) DO UPDATE SET title=EXCLUDED.title,tags=EXCLUDED.tags,description=EXCLUDED.description;
    END IF;
    -- Writes above can wait on another row lock past the lease deadline.
    -- Recheck after those waits; raising rolls back both payload and task state.
    UPDATE drama_archive.pipeline_tasks SET status='succeeded',lease_token=NULL,lease_expires_at=NULL,
        last_error=NULL,updated_at=clock_timestamp() WHERE video_id=vid AND stage=step
        AND status='running' AND lease_token=token AND lease_expires_at>clock_timestamp();
    IF NOT FOUND THEN RAISE EXCEPTION 'lease_lost'; END IF;
END $$;
COMMIT;
