-- Restricted read for the batch scheduler: message ids whose upload is committed.
-- Same hardening as 001: SECURITY DEFINER, pinned search_path, no PUBLIC execute.
CREATE FUNCTION drama_archive.archived_messages() RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
    SELECT coalesce(jsonb_agg(message_id::text ORDER BY message_id), '[]'::jsonb)
    FROM drama_archive.videos WHERE object_path IS NOT NULL
$$;
REVOKE ALL ON FUNCTION drama_archive.archived_messages() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drama_archive.archived_messages() TO drama_worker;
DO $$ DECLARE r text; BEGIN
    FOREACH r IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
        IF EXISTS (SELECT FROM pg_roles WHERE rolname=r) THEN
            EXECUTE format('REVOKE ALL ON FUNCTION drama_archive.archived_messages() FROM %I',r);
        END IF;
    END LOOP;
END $$;
