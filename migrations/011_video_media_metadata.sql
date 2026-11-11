-- Source duration for every video, and width/height/bitrate stored when a video is imported.
--
-- Before this, import_video() wrote only the four text columns, so every new row kept NULL
-- width/height/bitrate_bps until an out-of-band backfill ran. The importer now writes all four
-- media columns and, for an existing row, fills only the ones that are still NULL (never
-- overwrites). Malformed or non-positive values become NULL instead of failing the import, so
-- the constraints below can never reject a pipeline run.
-- No new function, role, or grant: Database.verify_worker() still sees the same five tables and
-- the same functions. Apply once as database owner, after 010. Existing rows keep NULL
-- duration_seconds until backfilled from the source inventory.
BEGIN;
ALTER TABLE drama_archive.videos ADD COLUMN duration_seconds numeric(10,3);
ALTER TABLE drama_archive.videos
    ADD CONSTRAINT videos_duration_positive CHECK (duration_seconds IS NULL OR duration_seconds > 0);

CREATE OR REPLACE FUNCTION drama_archive.import_video(d jsonb, v jsonb) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, drama_archive AS $$
DECLARE existing drama_archive.videos; did uuid; vid uuid;
        w integer; h integer; br integer; dur numeric(10,3);
BEGIN
    did := (d->>'id')::uuid; vid := (v->>'id')::uuid;
    IF (v->>'drama_id')::uuid IS DISTINCT FROM did THEN
        RAISE EXCEPTION 'drama_identity_conflict';
    END IF;
    -- Media metadata: a value is used only when it is a plain positive number in range.
    IF (v->>'width') ~ '^[1-9][0-9]{0,8}$' AND (v->>'height') ~ '^[1-9][0-9]{0,8}$' THEN
        w := (v->>'width')::integer; h := (v->>'height')::integer;
    END IF;
    IF (v->>'bitrate_bps') ~ '^[1-9][0-9]{0,8}$' THEN
        br := (v->>'bitrate_bps')::integer;
    END IF;
    IF (v->>'duration_seconds') ~ '^[0-9]{1,7}(\.[0-9]{1,3})?$' AND (v->>'duration_seconds')::numeric > 0 THEN
        dur := (v->>'duration_seconds')::numeric(10,3);
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
        original_title,original_tags,original_description,episode,
        width,height,bitrate_bps,duration_seconds)
      VALUES (vid,did,(v->>'source_id')::bigint,(v->>'message_id')::bigint,
        (v->>'media_id')::bigint,v->>'original_title',
        ARRAY(SELECT jsonb_array_elements_text(v->'original_tags')),
        v->>'original_description',v->>'episode',
        w,h,br,dur)
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
    -- Fill-only: an existing row never loses or changes a stored value. width/height move as a pair.
    IF (existing.width IS NULL AND existing.height IS NULL AND w IS NOT NULL)
       OR (existing.bitrate_bps IS NULL AND br IS NOT NULL)
       OR (existing.duration_seconds IS NULL AND dur IS NOT NULL) THEN
        UPDATE drama_archive.videos SET
            width = CASE WHEN width IS NULL AND height IS NULL THEN w ELSE width END,
            height = CASE WHEN width IS NULL AND height IS NULL THEN h ELSE height END,
            bitrate_bps = COALESCE(bitrate_bps, br),
            duration_seconds = COALESCE(duration_seconds, dur)
          WHERE id=vid;
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
COMMIT;
