INSERT INTO dds.dm_couriers (courier_id, courier_name)
SELECT 
    courier_id,
    courier_name
FROM stg.st_couriers AS sc
ON CONFLICT (courier_id) DO UPDATE
SET courier_name = EXCLUDED.courier_name
;