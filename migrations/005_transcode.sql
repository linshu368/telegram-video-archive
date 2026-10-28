-- Backlog remux: move the moov atom to the front of already-archived videos
-- whose upload predates faststart (ffmpeg -c copy, lossless). object_path never
-- changes -- the object is overwritten in place, not re-identified.
-- Apply as database owner on an existing drama_archive schema (pipeline.py migrate
-- --migration 005_transcode.sql). Table set, function set, owners and grants are
-- otherwise untouched, so Database.verify_worker()'s contract still holds.
BEGIN;
ALTER TABLE drama_archive.videos
    ADD COLUMN transcoded_at timestamptz;

ALTER TABLE drama_archive.pipeline_tasks DROP CONSTRAINT pipeline_tasks_stage_check;
ALTER TABLE drama_archive.pipeline_tasks ADD CONSTRAINT pipeline_tasks_stage_check
    CHECK (stage IN ('download', 'upload', 'ai', 'cover_download', 'cover_upload', 'transcode'));

-- import_video() is deliberately NOT changed: new videos never get a 'transcode'
-- row, so the backlog stays scoped to what this backfill inserts below. If a
-- future video ever needs it, that is a separate decision, not an automatic one.
INSERT INTO drama_archive.pipeline_tasks(video_id, stage)
    SELECT id, 'transcode' FROM drama_archive.videos
    WHERE object_path IS NOT NULL AND transcoded_at IS NULL
    ON CONFLICT DO NOTHING;

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
             WHERE id=vid AND cover_path IS NULL AND cover_note IS NULL))
        OR (step='transcode' AND EXISTS (SELECT FROM drama_archive.videos
             WHERE id=vid AND object_path IS NOT NULL AND transcoded_at IS NULL)))
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
    ELSIF step='transcode' THEN
        -- object_path is unchanged by design; only the moov position moved.
        UPDATE drama_archive.videos SET transcoded_at=clock_timestamp() WHERE id=vid;
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
