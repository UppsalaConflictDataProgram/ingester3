DROP EVENT TRIGGER IF EXISTS drop_cache;
DROP EVENT TRIGGER IF EXISTS update_cache;

CREATE OR REPLACE FUNCTION void_cache_timestamp() RETURNS event_trigger AS $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.schema_name = 'prod' THEN
        UPDATE prod_metadata.update_stamp SET ddl_stamp = extract(epoch from now())::bigint;
        UPDATE prod_metadata.update_stamp SET data_stamp = extract(epoch from now())::bigint;
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
        UPDATE prod_metadata.update_stamp SET ddl_stamp = extract(epoch from now())::bigint;
        UPDATE prod_metadata.update_stamp SET data_stamp = extract(epoch from now())::bigint;
        END IF;
    END LOOP;
END;
$$;
CREATE EVENT TRIGGER drop_cache
   ON sql_drop
   EXECUTE FUNCTION void_cache_drop();



DO $$
declare tabs RECORD;
declare x text;
BEGIN
for tabs IN
(select table_name,table_schema
   from information_schema.tables where table_schema like 'prod'
  -- and table_schema like '%'
) LOOP
x = format('CREATE TRIGGER check_update_%I AFTER UPDATE ON' ||
           ' prod.%I FOR EACH STATEMENT EXECUTE PROCEDURE update_timestamp()',
    tabs.table_name,tabs.table_name);
execute x;
raise notice ':%',x;
END LOOP;
END $$;


CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS
    $$
    BEGIN
    UPDATE prod_metadata.update_stamp SET data_stamp = extract(epoch from now())::bigint;
    RAISE NOTICE 'UPDATE ON prod, TRIGGERED update_timestamp()';
    RETURN NULL;
    end
    $$ LANGUAGE plpgsql;
