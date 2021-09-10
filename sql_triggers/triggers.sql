DROP EVENT TRIGGER IF EXISTS drop_cache;
DROP EVENT TRIGGER IF EXISTS update_cache;

CREATE OR REPLACE FUNCTION void_cache_timestamp() RETURNS event_trigger AS $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.schema_name = 'prod' THEN
        UPDATE prod_metadata.update_stamp SET update_stamp = extract(epoch from now())::bigint;
        RAISE NOTICE 'Working on prod DDL will trigger an event on cache for : %', r.object_identity;
        END IF;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE EVENT TRIGGER update_cache
  ON ddl_command_end WHEN TAG IN ('ALTER TABLE','CREATE TABLE AS','DROP SCHEMA','CREATE TABLE')
  EXECUTE PROCEDURE void_cache_timestamp();



CREATE OR REPLACE FUNCTION void_cache_drop() RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF obj.schema_name = 'prod' THEN
        RAISE NOTICE 'Deleting on prod DDL will trigger an event on cache for : %', obj.object_identity;
        UPDATE prod_metadata.update_stamp SET update_stamp = extract(epoch from now())::bigint;
        END IF;
    END LOOP;
END;
$$;
CREATE EVENT TRIGGER drop_cache
   ON sql_drop
   EXECUTE FUNCTION void_cache_drop();