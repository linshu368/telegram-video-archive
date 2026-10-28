-- Fix: concurrent import of videos of the same drama raised dramas_pkey UniqueViolation.
-- Replaces only the function body; owner, grants and signature are unchanged.
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
      SELECT vid, unnest(ARRAY['download','upload','ai']) ON CONFLICT DO NOTHING;
    RETURN vid;
END $$;
