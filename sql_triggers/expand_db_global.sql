CREATE TABLE public.temp_months_w_changes AS
with a AS (
SELECT month_start as month FROM prod.country UNION
SELECT month_end as month FROM prod.country
), b AS (
    SELECT month FROM a UNION
    SELECT month -1 FROM a UNION
    SELECT month +1 FROM a WHERE month
   > 2
)
SELECT 1 as month_id UNION SELECT * FROM  b WHERE month>1;

DO $$
DECLARE months INT;
DECLARE ftxt TEXT;
DECLARE ffil TEXT;
BEGIN

ftxt = 'CREATE TABLE public.month_%s AS
with a AS (
    SELECT cm.id as cm_id,
           c.id  as c_id,
           geom
    FROM prod.country_month cm,
         prod.country c
    WHERE month_id = %L
      AND cm.country_id = c.id
),
b AS (
    SELECT pgm.id as pgm_id,
           pg.id  as pg_id,
           geom
    FROM prod.priogrid_month pgm,
         prod.priogrid pg
    WHERE month_id = %L
      AND pg.id = pgm.priogrid_gid
),
intersection AS (
    SELECT cm_id, c_id, pgm_id, pg_id,
           ST_Area( st_intersection(a.geom,b.geom)) as area
    FROM a,
         b
    -- WHERE ST_Intersects(a.geom, b.geom)
    WHERE a.geom && b.geom
),
cself AS (
    SELECT 300 as month_id, pg_id, c_id
    FROM (
             SELECT *, rank() over (partition by pgm_id order by area desc) as rnk FROM intersection
         ) rnk
    WHERE rnk = 1)
SELECT * FROM cself';

for months IN SELECT * FROM temp_months_w_changes ORDER BY month_id
    LOOP
    ffil = format(ftxt,months,months,months);
    RAISE NOTICE 'months %', months;
    RAISE NOTICE '********************';
    RAISE NOTICE '%',ffil;
    EXECUTE format(ftxt,months,months,months);
    END LOOP;
end;
$$;


DO $$
DECLARE months INT;
DECLARE lookup INT;
DECLARE qtext TEXT;
BEGIN
FOR months IN SELECT  generate_series(1,860) as s ORDER BY s LOOP
    SELECT max(month_id) INTO lookup FROM temp_months_w_changes WHERE month_id <= months;
    RAISE NOTICE 'month % : lookup %', months, lookup;
    qtext = 'with a as (
    SELECT ci.pg_id, ci.c_id, cm.id as cm_id, pgm.id as pgm_id
    FROM month_%s ci,
         prod.priogrid_month pgm,
         prod.country_month cm
    WHERE ci.pg_id = pgm.priogrid_gid
      AND pgm.month_id = %s
      AND ci.c_id = cm.country_id
      AND cm.month_id = %s
) UPDATE
    prod.priogrid_month SET
    country_month_id=a.cm_id
    FROM a
    WHERE month_id = %s
    AND id = a.pgm_id;';
    qtext = format(qtext,lookup,months,months,months);
    --RAISE NOTICE '%',qtext;
    execute (qtext);
END LOOP;
END;
$$;
