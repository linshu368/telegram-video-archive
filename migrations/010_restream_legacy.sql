-- Apply after 009. Re-streams legacy Supabase videos to Cloudflare Stream, one whole drama at a time.
--
-- A legacy row (storage_provider='supabase', object_path set) keeps pointing at its old file, so the
-- App keeps playing it, for the whole re-upload. begin_restream() marks it and resets download/upload.
-- While marked, video_state() reports the row as a Stream row (the worker needs no new branches).
-- Stream readiness is parked in restream_ready_at; the last finish(stream_process) of a drama flips
-- every one of its rows in one transaction:
--   storage_provider='cloudflare_stream', stream_uid/stream_ready_at set, object_path NULL.
-- The old file is never deleted. Its path is always 'videos/<id>/original'. Rollback of a flipped row:
--   UPDATE drama_archive.videos SET storage_provider='supabase', object_path='videos/'||id||'/original',
--          stream_uid=NULL, stream_ready_at=NULL WHERE id = ANY(...);
BEGIN;
ALTER TABLE drama_archive.videos
    ADD COLUMN restream_at timestamptz,
    ADD COLUMN restream_ready_at timestamptz,
    ADD CHECK (restream_ready_at IS NULL OR restream_at IS NOT NULL);

CREATE FUNCTION drama_archive.begin_restream(vid uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    IF EXISTS (SELECT FROM drama_archive.pipeline_tasks
               WHERE video_id=vid AND status='running' AND lease_expires_at>clock_timestamp()) THEN
        RETURN false;
    END IF;
    UPDATE drama_archive.videos SET restream_at=clock_timestamp()
        WHERE id=vid AND storage_provider='supabase' AND object_path IS NOT NULL AND restream_at IS NULL;
    IF NOT FOUND THEN RETURN false; END IF;
    UPDATE drama_archive.pipeline_tasks SET status='pending',attempts=0,last_error=NULL,
        lease_token=NULL,lease_expires_at=NULL,stream_create_started=false,uploaded_stream_uid=NULL,
        source_sha256=NULL,source_bytes=NULL,updated_at=clock_timestamp()
        WHERE video_id=vid AND stage IN ('download','upload');
    INSERT INTO drama_archive.pipeline_tasks(video_id,stage) VALUES (vid,'stream_process')
        ON CONFLICT DO NOTHING;
    RETURN true;
END $$;

-- Message ids of every video still served from Supabase, including ones being re-streamed.
CREATE FUNCTION drama_archive.legacy_messages() RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT coalesce(jsonb_agg(message_id::text ORDER BY message_id),'[]'::jsonb)
    FROM drama_archive.videos WHERE storage_provider='supabase' AND object_path IS NOT NULL
$$;

DO $$ DECLARE r text; f text; BEGIN
    FOREACH f IN ARRAY ARRAY['begin_restream(uuid)','legacy_messages()'] LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION drama_archive.%s FROM PUBLIC',f);
        EXECUTE format('GRANT EXECUTE ON FUNCTION drama_archive.%s TO drama_worker',f);
        FOREACH r IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
            IF EXISTS (SELECT FROM pg_roles WHERE rolname=r) THEN
                EXECUTE format('REVOKE ALL ON FUNCTION drama_archive.%s FROM %I',f,r);
            END IF;
        END LOOP;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION drama_archive.video_state(vid uuid) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT to_jsonb(v)
        || CASE WHEN v.storage_provider='supabase' AND v.restream_at IS NOT NULL THEN
               jsonb_build_object('storage_provider','cloudflare_stream','object_path',NULL::text,
                   'stream_uid', CASE WHEN v.restream_ready_at IS NOT NULL THEN
                       (SELECT uploaded_stream_uid FROM drama_archive.pipeline_tasks WHERE video_id=v.id AND stage='upload') END,
                   'stream_ready_at', v.restream_ready_at)
           ELSE '{}'::jsonb END
        || jsonb_build_object('tasks',
            (SELECT jsonb_object_agg(stage,to_jsonb(t)) FROM drama_archive.pipeline_tasks t WHERE t.video_id=vid)
              || jsonb_build_object('ai', (SELECT to_jsonb(dt) FROM drama_archive.drama_tasks dt WHERE dt.drama_id=v.drama_id)),
            'ai', (SELECT to_jsonb(a) FROM drama_archive.ai_results a WHERE a.drama_id=v.drama_id))
    FROM drama_archive.videos v WHERE v.id=vid
$$;

-- Legacy Supabase files no longer count as archived; a re-streamed video does once Stream is ready.
CREATE OR REPLACE FUNCTION drama_archive.archived_messages() RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT coalesce(jsonb_agg(message_id::text ORDER BY message_id),'[]'::jsonb)
    FROM drama_archive.videos WHERE stream_uid IS NOT NULL OR restream_ready_at IS NOT NULL
$$;

-- AI reads a video that is playable now: not a legacy row whose re-stream is still running.
CREATE OR REPLACE FUNCTION drama_archive.ai_source_video(did uuid) RETURNS uuid
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT id FROM drama_archive.videos WHERE drama_id=did
      AND (stream_uid IS NOT NULL OR restream_ready_at IS NOT NULL OR (object_path IS NOT NULL AND restream_at IS NULL))
    ORDER BY message_id ASC LIMIT 1
$$;

CREATE OR REPLACE FUNCTION drama_archive.stream_create_intent(vid uuid, token uuid, creating boolean) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    UPDATE drama_archive.pipeline_tasks SET stream_create_started=creating
        WHERE video_id=vid AND stage='upload' AND status='running' AND lease_token=token
          AND lease_expires_at>clock_timestamp() AND stream_create_started <> creating
          AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid
                      AND (storage_provider='cloudflare_stream' OR restream_at IS NOT NULL));
    RETURN FOUND;
END $$;

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
          AND EXISTS (SELECT FROM drama_archive.videos WHERE drama_id=vid
              AND (stream_uid IS NOT NULL OR restream_ready_at IS NOT NULL OR (object_path IS NOT NULL AND restream_at IS NULL)))
        RETURNING lease_token INTO result;
        RETURN result;
    END IF;
    UPDATE drama_archive.pipeline_tasks t SET status='running', attempts=attempts+1,
        lease_token=token, lease_expires_at=clock_timestamp()+make_interval(secs=>seconds),
        updated_at=clock_timestamp(), last_error=NULL
    WHERE t.video_id=vid AND t.stage=step AND t.attempts<max_attempts AND
      (t.status='pending' OR (t.status='failed' AND t.updated_at <= clock_timestamp()-make_interval(secs=>backoff))
       OR (t.status='running' AND t.lease_expires_at <= clock_timestamp()))
      AND ((step='download' AND EXISTS (SELECT FROM drama_archive.videos
             WHERE id=vid AND (object_path IS NULL OR restream_at IS NOT NULL) AND stream_uid IS NULL))
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
        restreaming boolean; did uuid;
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
          WHERE id=NULLIF(payload->>'source_video_id','')::uuid AND drama_id=vid
            AND (stream_uid IS NOT NULL OR restream_ready_at IS NOT NULL OR (object_path IS NOT NULL AND restream_at IS NULL));
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
    -- A legacy row being re-streamed follows the Stream branches while it still points at its old file.
    SELECT storage_provider='supabase' AND restream_at IS NOT NULL, drama_id INTO restreaming, did
        FROM drama_archive.videos WHERE id=vid;
    IF step IN ('upload','stream_process') AND payload->>'pending' IN ('encoding','reconciliation')
       AND EXISTS (SELECT FROM drama_archive.videos WHERE id=vid
                   AND (storage_provider='cloudflare_stream' OR restream_at IS NOT NULL)) THEN
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
        IF restreaming THEN
            -- Lock the whole drama before touching any row, in id order, so two episodes finishing
            -- together cannot deadlock and the second one sees the first one's readiness.
            PERFORM 1 FROM drama_archive.videos WHERE drama_id=did ORDER BY id FOR UPDATE;
            UPDATE drama_archive.videos v SET restream_ready_at=clock_timestamp()
              FROM drama_archive.pipeline_tasks u WHERE v.id=vid AND v.storage_provider='supabase'
              AND v.restream_at IS NOT NULL AND v.restream_ready_at IS NULL
              AND u.video_id=vid AND u.stage='upload' AND u.status='succeeded'
              AND u.uploaded_stream_uid=payload->>'uid';
        ELSE
            UPDATE drama_archive.videos v SET stream_uid=u.uploaded_stream_uid,stream_ready_at=clock_timestamp()
              FROM drama_archive.pipeline_tasks u WHERE v.id=vid AND v.storage_provider='cloudflare_stream'
              AND v.stream_uid IS NULL AND u.video_id=vid AND u.stage='upload' AND u.status='succeeded'
              AND u.uploaded_stream_uid=payload->>'uid';
        END IF;
        IF NOT FOUND THEN RAISE EXCEPTION 'stream_identity_conflict'; END IF;
    ELSIF step='upload' AND EXISTS (SELECT FROM drama_archive.videos
                                    WHERE id=vid AND (storage_provider='cloudflare_stream' OR restream_at IS NOT NULL)) THEN
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
    -- The drama's last re-streamed episode switches every row at once: the App never sees a half-moved drama.
    IF step='stream_process' AND restreaming
       AND NOT EXISTS (SELECT FROM drama_archive.videos WHERE drama_id=did
                       AND NOT (stream_uid IS NOT NULL OR (storage_provider='supabase' AND restream_ready_at IS NOT NULL))) THEN
        UPDATE drama_archive.videos v SET storage_provider='cloudflare_stream',
            stream_uid=u.uploaded_stream_uid, stream_ready_at=v.restream_ready_at, object_path=NULL
          FROM drama_archive.pipeline_tasks u
          WHERE v.drama_id=did AND v.storage_provider='supabase' AND v.restream_ready_at IS NOT NULL
            AND u.video_id=v.id AND u.stage='upload' AND u.status='succeeded' AND u.uploaded_stream_uid IS NOT NULL;
    END IF;
END $$;
COMMIT;
