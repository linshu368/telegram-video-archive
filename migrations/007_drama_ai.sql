-- AI-generated title/tags/description move from per-video to per-drama: every
-- video under the same drama now shares one ai_results row, because that is
-- the display granularity the miniapp actually shows (one card per drama).
-- The 'ai' stage moves off the per-video pipeline_tasks table onto a new
-- drama_tasks table, so the lease is a single atomic row per drama (no
-- cross-video locking needed to stop two workers generating two results).
-- Apply as database owner on an existing drama_archive schema that already
-- has 001-006 applied (pipeline.py migrate --migration 007_drama_ai.sql).
BEGIN;

-- dramas gains room for source-provided metadata from catalogs other than this
-- Telegram import, which may already ship a complete title/tags/description at
-- the drama level. Nothing populates these yet; title is unaffected.
ALTER TABLE drama_archive.dramas
    ADD COLUMN tags text[],
    ADD COLUMN description text;

CREATE TABLE drama_archive.drama_tasks (
    drama_id uuid PRIMARY KEY REFERENCES drama_archive.dramas(id),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text CHECK (last_error IS NULL OR last_error ~ '^[A-Za-z0-9_]{1,80}$'),
    lease_token uuid,
    lease_expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL))
);
CREATE INDEX drama_tasks_status ON drama_archive.drama_tasks(status, updated_at);
CREATE INDEX drama_tasks_expiry ON drama_archive.drama_tasks(lease_expires_at) WHERE status = 'running';
ALTER TABLE drama_archive.drama_tasks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON drama_archive.drama_tasks FROM PUBLIC, drama_worker;

-- One row per existing drama, seeded from whatever already resolved under the
-- old per-video scheme (see the ai_results rebuild below for the tie-break).
INSERT INTO drama_archive.drama_tasks(drama_id, status, attempts)
    SELECT d.id, 'pending', 0 FROM drama_archive.dramas d;

-- Rebuild ai_results around drama_id. Capture the tie-break choice (the video
-- whose 'ai' task most recently succeeded) before anything is dropped.
CREATE TEMP TABLE _drama_ai_winner ON COMMIT DROP AS
    SELECT DISTINCT ON (v.drama_id) v.drama_id, a.video_id AS source_video_id,
           a.title, a.tags, a.description
    FROM drama_archive.ai_results a
    JOIN drama_archive.videos v ON v.id = a.video_id
    LEFT JOIN drama_archive.pipeline_tasks t ON t.video_id = a.video_id AND t.stage = 'ai'
    ORDER BY v.drama_id, t.updated_at DESC NULLS LAST, a.video_id;

ALTER TABLE drama_archive.ai_results DROP CONSTRAINT ai_results_pkey;
ALTER TABLE drama_archive.ai_results
    ADD COLUMN drama_id uuid,
    ADD COLUMN source_video_id uuid;
UPDATE drama_archive.ai_results a SET drama_id = w.drama_id, source_video_id = w.source_video_id
    FROM _drama_ai_winner w WHERE w.source_video_id = a.video_id;
-- Rows belonging to a video that lost its drama's tie-break: superseded, not kept.
DELETE FROM drama_archive.ai_results WHERE drama_id IS NULL;
ALTER TABLE drama_archive.ai_results DROP COLUMN video_id;
ALTER TABLE drama_archive.ai_results
    ALTER COLUMN drama_id SET NOT NULL,
    ADD CONSTRAINT ai_results_pkey PRIMARY KEY (drama_id),
    ADD CONSTRAINT ai_results_drama_fkey FOREIGN KEY (drama_id) REFERENCES drama_archive.dramas(id),
    ADD CONSTRAINT ai_results_source_video_fkey FOREIGN KEY (source_video_id) REFERENCES drama_archive.videos(id);

-- Dramas that already had a winning result start their drama_task 'succeeded'.
UPDATE drama_archive.drama_tasks dt SET status = 'succeeded', attempts = 1
    FROM drama_archive.ai_results a WHERE a.drama_id = dt.drama_id;

-- 'ai' no longer lives on the per-video task table.
DELETE FROM drama_archive.pipeline_tasks WHERE stage = 'ai';
ALTER TABLE drama_archive.pipeline_tasks DROP CONSTRAINT pipeline_tasks_stage_check;
ALTER TABLE drama_archive.pipeline_tasks ADD CONSTRAINT pipeline_tasks_stage_check
    CHECK (stage IN ('download', 'upload', 'cover_download', 'cover_upload', 'transcode'));

-- import_video(): create the drama's single ai task alongside the drama itself;
-- stop handing out a per-video 'ai' row.
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
    INSERT INTO drama_archive.drama_tasks(drama_id) VALUES (did) ON CONFLICT DO NOTHING;
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
      SELECT vid, unnest(ARRAY['download','upload','cover_download','cover_upload'])
      ON CONFLICT DO NOTHING;
    RETURN vid;
END $$;

-- video_state(): tasks.ai / ai keep their old shape and keys, but now read
-- through the video's drama_id instead of the video's own row, so every
-- existing caller that reads cloud['tasks']['ai'] / cloud['ai'] is unaffected.
CREATE OR REPLACE FUNCTION drama_archive.video_state(vid uuid) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT to_jsonb(v) || jsonb_build_object('tasks',
        (SELECT jsonb_object_agg(stage,to_jsonb(t)) FROM drama_archive.pipeline_tasks t WHERE t.video_id=vid)
          || jsonb_build_object('ai', (SELECT to_jsonb(dt) FROM drama_archive.drama_tasks dt WHERE dt.drama_id=v.drama_id)),
        'ai', (SELECT to_jsonb(a) FROM drama_archive.ai_results a WHERE a.drama_id=v.drama_id))
    FROM drama_archive.videos v WHERE v.id=vid
$$;

-- Which video's frames/transcript actually feed the drama's one AI call:
-- deterministic (earliest message id), read-only, re-checked at finish time.
CREATE FUNCTION drama_archive.ai_source_video(did uuid) RETURNS uuid
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT id FROM drama_archive.videos WHERE drama_id=did AND object_path IS NOT NULL
    ORDER BY message_id ASC LIMIT 1
$$;
REVOKE ALL ON FUNCTION drama_archive.ai_source_video(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drama_archive.ai_source_video(uuid) TO drama_worker;

CREATE OR REPLACE FUNCTION drama_archive.claim(vid uuid, step text, seconds integer, max_attempts integer, backoff integer)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
DECLARE token uuid := gen_random_uuid(); result uuid;
BEGIN
    IF seconds NOT BETWEEN 5 AND 3600 OR max_attempts < 1 OR backoff < 0 THEN
        RAISE EXCEPTION 'invalid_lease_configuration';
    END IF;
    IF step = 'ai' THEN
        -- vid is a drama_id for this stage: the result is shared by every video under it.
        UPDATE drama_archive.drama_tasks t SET status='running', attempts=attempts+1,
            lease_token=token, lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
            updated_at=clock_timestamp(), last_error=NULL
        WHERE t.drama_id=vid AND t.attempts<max_attempts AND
          (t.status='pending' OR (t.status='failed' AND t.updated_at <= clock_timestamp()-make_interval(secs=>backoff))
           OR (t.status='running' AND t.lease_expires_at <= clock_timestamp()))
          AND EXISTS (SELECT FROM drama_archive.videos WHERE drama_id=vid AND object_path IS NOT NULL)
        RETURNING lease_token INTO result;
        RETURN result;
    END IF;
    UPDATE drama_archive.pipeline_tasks t SET status='running', attempts=attempts+1,
        lease_token=token, lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
        updated_at=clock_timestamp(), last_error=NULL
    WHERE t.video_id=vid AND t.stage=step AND t.attempts<max_attempts AND
      (t.status='pending' OR (t.status='failed' AND t.updated_at <= clock_timestamp()-make_interval(secs=>backoff))
       OR (t.status='running' AND t.lease_expires_at <= clock_timestamp()))
      AND ((step='download' AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND object_path IS NULL))
        OR (step='upload' AND EXISTS (SELECT FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage='download' AND status='succeeded'))
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

CREATE OR REPLACE FUNCTION drama_archive.renew(vid uuid, step text, token uuid, seconds integer) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    IF seconds NOT BETWEEN 5 AND 3600 THEN RAISE EXCEPTION 'invalid_lease_configuration'; END IF;
    IF step = 'ai' THEN
        UPDATE drama_archive.drama_tasks SET lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
          updated_at=clock_timestamp() WHERE drama_id=vid AND status='running'
          AND lease_token=token AND lease_expires_at>clock_timestamp();
        RETURN FOUND;
    END IF;
    UPDATE drama_archive.pipeline_tasks SET lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
      updated_at=clock_timestamp() WHERE video_id=vid AND stage=step AND status='running'
      AND lease_token=token AND lease_expires_at>clock_timestamp();
    RETURN FOUND;
END $$;

CREATE OR REPLACE FUNCTION drama_archive.fail(vid uuid, step text, token uuid, code text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    IF step = 'ai' THEN
        UPDATE drama_archive.drama_tasks SET status='failed',last_error=code,lease_token=NULL,
          lease_expires_at=NULL,updated_at=clock_timestamp() WHERE drama_id=vid
          AND status='running' AND lease_token=token AND lease_expires_at>clock_timestamp();
        RETURN FOUND;
    END IF;
    UPDATE drama_archive.pipeline_tasks SET status='failed',last_error=code,lease_token=NULL,
      lease_expires_at=NULL,updated_at=clock_timestamp() WHERE video_id=vid AND stage=step
      AND status='running' AND lease_token=token AND lease_expires_at>clock_timestamp();
    RETURN FOUND;
END $$;

CREATE OR REPLACE FUNCTION drama_archive.finish(vid uuid, step text, token uuid, payload jsonb DEFAULT '{}') RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
DECLARE task drama_archive.pipeline_tasks; dtask drama_archive.drama_tasks; source_video drama_archive.videos;
BEGIN
    IF step = 'ai' THEN
        -- vid is a drama_id here; the result is shared by every video under it.
        SELECT * INTO dtask FROM drama_archive.drama_tasks WHERE drama_id=vid FOR UPDATE;
        IF dtask.status IS DISTINCT FROM 'running' OR dtask.lease_token IS DISTINCT FROM token
            OR dtask.lease_expires_at <= clock_timestamp() THEN RAISE EXCEPTION 'lease_lost'; END IF;
        IF jsonb_typeof(payload->'tags') IS DISTINCT FROM 'array'
           OR EXISTS (SELECT FROM jsonb_array_elements(payload->'tags') x WHERE jsonb_typeof(x) <> 'string')
           OR (payload->'title' IS NOT NULL AND jsonb_typeof(payload->'title') NOT IN ('string','null'))
           OR (payload->'description' IS NOT NULL AND jsonb_typeof(payload->'description') NOT IN ('string','null')) THEN
            RAISE EXCEPTION 'invalid_ai_output';
        END IF;
        SELECT * INTO source_video FROM drama_archive.videos
          WHERE id=NULLIF(payload->>'source_video_id','')::uuid AND drama_id=vid AND object_path IS NOT NULL;
        IF source_video.id IS NULL THEN RAISE EXCEPTION 'ai_source_video_conflict'; END IF;
        INSERT INTO drama_archive.ai_results(drama_id,source_video_id,title,tags,description)
          VALUES (vid,source_video.id,NULLIF(btrim(payload->>'title'),''),ARRAY(SELECT DISTINCT btrim(x)
            FROM jsonb_array_elements_text(payload->'tags') x WHERE btrim(x)<>''),NULLIF(btrim(payload->>'description'),''))
          ON CONFLICT (drama_id) DO UPDATE SET source_video_id=EXCLUDED.source_video_id,
            title=EXCLUDED.title,tags=EXCLUDED.tags,description=EXCLUDED.description;
        UPDATE drama_archive.drama_tasks SET status='succeeded',lease_token=NULL,lease_expires_at=NULL,
            last_error=NULL,updated_at=clock_timestamp() WHERE drama_id=vid
            AND status='running' AND lease_token=token AND lease_expires_at>clock_timestamp();
        IF NOT FOUND THEN RAISE EXCEPTION 'lease_lost'; END IF;
        RETURN;
    END IF;
    SELECT * INTO task FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage=step FOR UPDATE;
    IF task.status IS DISTINCT FROM 'running' OR task.lease_token IS DISTINCT FROM token
        OR task.lease_expires_at <= clock_timestamp() THEN RAISE EXCEPTION 'lease_lost'; END IF;
    IF step='upload' THEN
        IF payload->>'object_path' IS DISTINCT FROM 'videos/' || vid::text || '/original' THEN
            RAISE EXCEPTION 'object_identity_conflict';
        END IF;
        UPDATE drama_archive.videos SET object_path=payload->>'object_path' WHERE id=vid;
        IF NOT EXISTS (SELECT FROM drama_archive.drama_tasks dt JOIN drama_archive.videos v
                       ON v.drama_id=dt.drama_id WHERE v.id=vid) THEN
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
    END IF;
    -- Writes above can wait on another row lock past the lease deadline.
    -- Recheck after those waits; raising rolls back both payload and task state.
    UPDATE drama_archive.pipeline_tasks SET status='succeeded',lease_token=NULL,lease_expires_at=NULL,
        last_error=NULL,updated_at=clock_timestamp() WHERE video_id=vid AND stage=step
        AND status='running' AND lease_token=token AND lease_expires_at>clock_timestamp();
    IF NOT FOUND THEN RAISE EXCEPTION 'lease_lost'; END IF;
END $$;

-- regenerate_ai(): vid is now a drama_id, mirroring the rest of the 'ai' stage.
CREATE OR REPLACE FUNCTION drama_archive.regenerate_ai(vid uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    UPDATE drama_archive.drama_tasks SET status='pending',attempts=0,last_error=NULL,
      updated_at=clock_timestamp() WHERE drama_id=vid AND status IN ('succeeded','failed');
    RETURN FOUND;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA drama_archive FROM PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA drama_archive TO drama_worker;
DO $$ DECLARE r text; BEGIN
    FOREACH r IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
        IF EXISTS (SELECT FROM pg_roles WHERE rolname=r) THEN
            EXECUTE format('REVOKE ALL ON drama_archive.drama_tasks FROM %I',r);
        END IF;
    END LOOP;
END $$;
COMMIT;
