ALTER TABLE public.maria_tweets
    OWNER to labuser;

GRANT SELECT, UPDATE ON TABLE public.maria_tweets TO dharma_lab;

GRANT ALL ON TABLE public.maria_tweets TO labuser;

-- Index: _em_combined_text_gin_idx

-- DROP INDEX public._em_combined_text_gin_idx;

CREATE INDEX maria_em_combined_text_gin_idx
    ON public.maria_tweets USING gin
    (_em_combined_text COLLATE pg_catalog."default" gin_trgm_ops)
    TABLESPACE pg_default;

-- Index: collection_geo_idx

-- DROP INDEX public.collection_geo_idx;

CREATE INDEX maria_collection_geo_idx
    ON public.maria_tweets USING btree
    (collection_geo)
    TABLESPACE pg_default;

-- Index: created_at_idx

-- DROP INDEX public.created_at_idx;

CREATE INDEX maria_created_at_idx
    ON public.maria_tweets USING btree
    (created_at)
    TABLESPACE pg_default;

-- Index: id_idx

-- DROP INDEX public.id_idx;

CREATE UNIQUE INDEX maria_id_idx
    ON public.maria_tweets USING btree
    (id)
    TABLESPACE pg_default;

-- Index: text_gin_idx

-- DROP INDEX public.text_gin_idx;

CREATE INDEX maria_text_gin_idx
    ON public.maria_tweets USING gin
    (text COLLATE pg_catalog."default" gin_trgm_ops)
    TABLESPACE pg_default;

-- Index: text_idx

-- DROP INDEX public.text_idx;

CREATE INDEX maria_text_idx
    ON public.maria_tweets USING btree
    (text COLLATE pg_catalog."default")
    TABLESPACE pg_default;

-- Index: user_id_idx

-- DROP INDEX public.user_id_idx;

CREATE INDEX maria_user_id_idx
    ON public.maria_tweets USING btree
    (user_id)
    TABLESPACE pg_default;

-- Index: user_screen_name_idx

-- DROP INDEX public.user_screen_name_idx;

CREATE INDEX maria_user_screen_name_idx
    ON public.maria_tweets USING btree
    (user_screen_name COLLATE pg_catalog."default")
    TABLESPACE pg_default;

