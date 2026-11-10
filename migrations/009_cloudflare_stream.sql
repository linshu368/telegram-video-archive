-- Apply after 008. Existing rows retain their provider, including unfinished imports.
BEGIN;
ALTER TABLE drama_archive.videos
    ADD COLUMN storage_provider text NOT NULL DEFAULT 'supabase'
        CHECK (storage_provider IN ('supabase','cloudflare_stream')),
    ADD COLUMN stream_uid text UNIQUE CHECK (stream_uid ~ '^[0-9a-f]{32}$'),
    ADD COLUMN stream_ready_at timestamptz,
    ADD CHECK ((stream_uid IS NULL) = (stream_ready_at IS NULL)),
    ADD CHECK (CASE storage_provider WHEN 'supabase' THEN stream_uid IS NULL ELSE object_path IS NULL END);
ALTER TABLE drama_archive.videos ALTER COLUMN storage_provider SET DEFAULT 'cloudflare_stream';

ALTER TABLE drama_archive.pipeline_tasks
    DROP CONSTRAINT pipeline_tasks_stage_check,
    ADD CHECK (stage IN ('download','upload','cover_download','cover_upload','transcode','stream_process')),
    ADD COLUMN stream_create_started boolean NOT NULL DEFAULT false,
    ADD COLUMN uploaded_stream_uid text UNIQUE CHECK (uploaded_stream_uid ~ '^[0-9a-f]{32}$'),
    ADD COLUMN source_sha256 text CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    ADD COLUMN source_bytes bigint CHECK (source_bytes > 0);

-- Reserve exactly one remote create, including across machines or a lost response.
CREATE FUNCTION drama_archive.stream_create_intent(vid uuid, token uuid, creating boolean) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    UPDATE drama_archive.pipeline_tasks SET stream_create_started=creating
        WHERE video_id=vid AND stage='upload' AND status='running' AND lease_token=token
          AND lease_expires_at>clock_timestamp() AND stream_create_started <> creating
          AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND storage_provider='cloudflare_stream');
    RETURN FOUND;
END $$;
REVOKE ALL ON FUNCTION drama_archive.stream_create_intent(uuid,uuid,boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drama_archive.stream_create_intent(uuid,uuid,boolean) TO drama_worker;
DO $$ DECLARE r text; BEGIN
    FOREACH r IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
        IF EXISTS (SELECT FROM pg_roles WHERE rolname=r) THEN
            EXECUTE format('REVOKE ALL ON FUNCTION drama_archive.stream_create_intent(uuid,uuid,boolean) FROM %I',r);
        END IF;
    END LOOP;
END $$;

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
    IF NULLIF(btrim(existing.episode),'') IS NULL AND NULLIF(btrim(v->>'episode'),'') IS NOT NULL THEN
        UPDATE drama_archive.videos SET episode=v->>'episode'
          WHERE id=vid AND NULLIF(btrim(episode),'') IS NULL;
    END IF;
    INSERT INTO drama_archive.pipeline_tasks(video_id,stage)
      SELECT vid, unnest(ARRAY['download','upload','cover_download','cover_upload'])
      ON CONFLICT DO NOTHING;
    IF existing.storage_provider='cloudflare_stream' THEN
        INSERT INTO drama_archive.pipeline_tasks(video_id,stage) VALUES (vid,'stream_process')
            ON CONFLICT DO NOTHING;
    END IF;
    RETURN vid;
END $$;

CREATE OR REPLACE FUNCTION drama_archive.archived_messages() RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT coalesce(jsonb_agg(message_id::text ORDER BY message_id),'[]'::jsonb)
    FROM drama_archive.videos WHERE object_path IS NOT NULL OR stream_uid IS NOT NULL
$$;

CREATE OR REPLACE FUNCTION drama_archive.ai_source_video(did uuid) RETURNS uuid
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT id FROM drama_archive.videos WHERE drama_id=did AND (object_path IS NOT NULL OR stream_uid IS NOT NULL)
    ORDER BY message_id ASC LIMIT 1
$$;

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
          AND EXISTS (SELECT FROM drama_archive.videos WHERE drama_id=vid AND (object_path IS NOT NULL OR stream_uid IS NOT NULL))
        RETURNING lease_token INTO result;
        RETURN result;
    END IF;
    UPDATE drama_archive.pipeline_tasks t SET status='running', attempts=attempts+1,
        lease_token=token, lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
        updated_at=clock_timestamp(), last_error=NULL
    WHERE t.video_id=vid AND t.stage=step AND t.attempts<max_attempts AND
      (t.status='pending' OR (t.status='failed' AND t.updated_at <= clock_timestamp()-make_interval(secs=>backoff))
       OR (t.status='running' AND t.lease_expires_at <= clock_timestamp()))
      AND ((step='download' AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND object_path IS NULL AND stream_uid IS NULL))
        OR (step='upload' AND EXISTS (SELECT FROM drama_archive.pipeline_tasks WHERE video_id=vid AND stage='download' AND status='succeeded'))
        OR (step='stream_process' AND EXISTS (SELECT FROM drama_archive.pipeline_tasks
             WHERE video_id=vid AND stage='upload' AND status='succeeded' AND uploaded_stream_uid IS NOT NULL))
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
          WHERE id=NULLIF(payload->>'source_video_id','')::uuid AND drama_id=vid AND (object_path IS NOT NULL OR stream_uid IS NOT NULL);
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
    IF step IN ('upload','stream_process') AND payload->>'pending' IN ('encoding','reconciliation')
       AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid AND storage_provider='cloudflare_stream') THEN
        UPDATE drama_archive.pipeline_tasks SET status='pending',attempts=attempts-1,
            last_error=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
          WHERE video_id=vid AND stage=step AND lease_token=token AND lease_expires_at>clock_timestamp();
        IF NOT FOUND THEN RAISE EXCEPTION 'lease_lost'; END IF;
        RETURN;
    END IF;
    IF step='stream_process' THEN
        IF payload->>'state' IS DISTINCT FROM 'ready' OR (payload->>'pctComplete')::numeric IS DISTINCT FROM 100
           OR payload->'readyToStream' IS DISTINCT FROM 'true'::jsonb
           OR payload->'requireSignedURLs' IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION 'stream_not_ready';
        END IF;
        UPDATE drama_archive.videos v SET stream_uid=u.uploaded_stream_uid,stream_ready_at=clock_timestamp()
          FROM drama_archive.pipeline_tasks u WHERE v.id=vid AND v.storage_provider='cloudflare_stream'
          AND v.stream_uid IS NULL AND u.video_id=vid AND u.stage='upload' AND u.status='succeeded'
          AND u.uploaded_stream_uid=payload->>'uid';
        IF NOT FOUND THEN RAISE EXCEPTION 'stream_identity_conflict'; END IF;
    ELSIF step='upload' AND EXISTS (SELECT FROM drama_archive.videos
                                    WHERE id=vid AND storage_provider='cloudflare_stream') THEN
        IF payload->>'uid' IS NULL OR payload->>'source_sha256' IS NULL OR payload->>'source_bytes' IS NULL THEN
            RAISE EXCEPTION 'stream_upload_receipt_missing';
        END IF;
        UPDATE drama_archive.pipeline_tasks SET uploaded_stream_uid=payload->>'uid',
            source_sha256=payload->>'source_sha256',source_bytes=(payload->>'source_bytes')::bigint
          WHERE video_id=vid AND stage='upload';
    ELSIF step='upload' THEN
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
COMMIT;
