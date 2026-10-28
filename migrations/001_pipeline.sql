-- Apply once as database owner. Existing schema/resources are never replaced.
BEGIN;
CREATE SCHEMA drama_archive;
REVOKE ALL ON SCHEMA drama_archive FROM PUBLIC;
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'drama_worker') THEN
        CREATE ROLE drama_worker NOLOGIN NOSUPERUSER NOBYPASSRLS;
    ELSIF EXISTS (SELECT FROM pg_roles WHERE rolname='drama_worker'
                  AND (rolcanlogin OR rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR rolreplication))
          OR EXISTS (SELECT FROM pg_auth_members WHERE
              member=(SELECT oid FROM pg_roles WHERE rolname='drama_worker')
              OR roleid=(SELECT oid FROM pg_roles WHERE rolname='drama_worker'))
          OR EXISTS (SELECT FROM pg_shdepend WHERE refclassid='pg_authid'::regclass
              AND refobjid=(SELECT oid FROM pg_roles WHERE rolname='drama_worker')) THEN
        RAISE EXCEPTION 'existing_worker_role_conflict';
    END IF;
END $$;
CREATE TABLE drama_archive.dramas (
    id uuid PRIMARY KEY,
    title text NOT NULL CHECK (btrim(title) <> ''),
    source_id bigint NOT NULL,
    topic_id bigint NOT NULL CHECK (topic_id > 1),
    UNIQUE (source_id, topic_id), UNIQUE (id, source_id)
);
CREATE TABLE drama_archive.videos (
    id uuid PRIMARY KEY,
    drama_id uuid NOT NULL,
    source_id bigint NOT NULL,
    message_id bigint NOT NULL CHECK (message_id > 0),
    media_id bigint NOT NULL,
    original_title text,
    original_tags text[] NOT NULL DEFAULT '{}',
    original_description text,
    episode text,
    object_path text UNIQUE CHECK (object_path = 'videos/' || id::text || '/original'),
    UNIQUE (source_id, message_id),
    FOREIGN KEY (drama_id, source_id) REFERENCES drama_archive.dramas(id, source_id)
);
CREATE TABLE drama_archive.ai_results (
    video_id uuid PRIMARY KEY REFERENCES drama_archive.videos(id),
    title text,
    tags text[] NOT NULL DEFAULT '{}',
    description text
);
CREATE TABLE drama_archive.pipeline_tasks (
    video_id uuid NOT NULL REFERENCES drama_archive.videos(id),
    stage text NOT NULL CHECK (stage IN ('download', 'upload', 'ai')),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text CHECK (last_error IS NULL OR last_error ~ '^[A-Za-z0-9_]{1,80}$'),
    lease_token uuid,
    lease_expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (video_id, stage),
    CHECK ((status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL))
);
CREATE INDEX videos_drama ON drama_archive.videos(drama_id);
CREATE INDEX tasks_stage_status ON drama_archive.pipeline_tasks(stage, status, updated_at);
CREATE INDEX tasks_expiry ON drama_archive.pipeline_tasks(lease_expires_at) WHERE status = 'running';
ALTER TABLE drama_archive.dramas ENABLE ROW LEVEL SECURITY;
ALTER TABLE drama_archive.videos ENABLE ROW LEVEL SECURITY;
ALTER TABLE drama_archive.ai_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE drama_archive.pipeline_tasks ENABLE ROW LEVEL SECURITY;
-- No client policies. Worker gets only narrowly scoped SECURITY DEFINER functions.
REVOKE ALL ON ALL TABLES IN SCHEMA drama_archive FROM PUBLIC, drama_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA drama_archive REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

CREATE FUNCTION drama_archive.import_video(d jsonb, v jsonb) RETURNS uuid
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
      SELECT vid, unnest(ARRAY['download','upload','ai']) ON CONFLICT DO NOTHING;
    RETURN vid;
END $$;

CREATE FUNCTION drama_archive.video_state(vid uuid) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT to_jsonb(v) || jsonb_build_object('tasks',
        (SELECT jsonb_object_agg(stage,to_jsonb(t)) FROM drama_archive.pipeline_tasks t WHERE t.video_id=vid),
        'ai', (SELECT to_jsonb(a) FROM drama_archive.ai_results a WHERE a.video_id=vid))
    FROM drama_archive.videos v WHERE v.id=vid
$$;

CREATE FUNCTION drama_archive.claim(vid uuid, step text, seconds integer, max_attempts integer, backoff integer)
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
             AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND object_path IS NOT NULL)))
    RETURNING lease_token INTO result;
    RETURN result;
END $$;

CREATE FUNCTION drama_archive.renew(vid uuid, step text, token uuid, seconds integer) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    IF seconds NOT BETWEEN 5 AND 3600 THEN RAISE EXCEPTION 'invalid_lease_configuration'; END IF;
    UPDATE drama_archive.pipeline_tasks SET lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
      updated_at=clock_timestamp() WHERE video_id=vid AND stage=step AND status='running'
      AND lease_token=token AND lease_expires_at>clock_timestamp();
    RETURN FOUND;
END $$;

CREATE FUNCTION drama_archive.finish(vid uuid, step text, token uuid, payload jsonb DEFAULT '{}') RETURNS void
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

CREATE FUNCTION drama_archive.fail(vid uuid, step text, token uuid, code text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    UPDATE drama_archive.pipeline_tasks SET status='failed',last_error=code,lease_token=NULL,
      lease_expires_at=NULL,updated_at=clock_timestamp() WHERE video_id=vid AND stage=step
      AND status='running' AND lease_token=token AND lease_expires_at>clock_timestamp();
    RETURN FOUND;
END $$;

CREATE FUNCTION drama_archive.regenerate_ai(vid uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    UPDATE drama_archive.pipeline_tasks SET status='pending',attempts=0,last_error=NULL,
      updated_at=clock_timestamp() WHERE video_id=vid AND stage='ai' AND status IN ('succeeded','failed');
    RETURN FOUND;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA drama_archive FROM PUBLIC;
GRANT USAGE ON SCHEMA drama_archive TO drama_worker;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA drama_archive TO drama_worker;
-- Supabase API roles must not inherit default privileges on these resources.
DO $$ DECLARE r text; BEGIN
    FOREACH r IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
        IF EXISTS (SELECT FROM pg_roles WHERE rolname=r) THEN
            EXECUTE format('REVOKE ALL ON SCHEMA drama_archive FROM %I',r);
            EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA drama_archive FROM %I',r);
            EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA drama_archive FROM %I',r);
        END IF;
    END LOOP;
END $$;
COMMIT;
