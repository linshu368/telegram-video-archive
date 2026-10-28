-- Unstick a transcode task whose attempts were exhausted by transient network
-- failures (claim() refuses once attempts >= max_attempts, regardless of how
-- long ago that happened). Mirrors regenerate_ai(); also covers a 'running'
-- row whose lease has expired, since that one is stuck the same way.
-- Apply as database owner (pipeline.py migrate --migration 006_regenerate_transcode.sql).
BEGIN;
CREATE FUNCTION drama_archive.regenerate_transcode(vid uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
BEGIN
    UPDATE drama_archive.pipeline_tasks SET status='pending', attempts=0, last_error=NULL,
      lease_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
      WHERE video_id=vid AND stage='transcode'
        AND (status IN ('succeeded','failed') OR (status='running' AND lease_expires_at<=clock_timestamp()));
    RETURN FOUND;
END $$;
REVOKE ALL ON FUNCTION drama_archive.regenerate_transcode(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drama_archive.regenerate_transcode(uuid) TO drama_worker;
COMMIT;
