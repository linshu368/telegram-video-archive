-- Display width, height, and average bitrate for videos already imported.
-- Values stay NULL until backfill_video_dimensions.py writes them from
-- archive/index.csv. No new function, role, or grant: Database.verify_worker()
-- still sees the same five tables and ten functions.
-- Apply once as database owner on main.
BEGIN;
ALTER TABLE drama_archive.videos
    ADD COLUMN width integer,
    ADD COLUMN height integer,
    ADD COLUMN bitrate_bps integer;

ALTER TABLE drama_archive.videos
    ADD CONSTRAINT videos_dimensions_pair CHECK (
        (width IS NULL AND height IS NULL) OR (width > 0 AND height > 0)
    ),
    ADD CONSTRAINT videos_bitrate_positive CHECK (
        bitrate_bps IS NULL OR bitrate_bps > 0
    );
COMMIT;
