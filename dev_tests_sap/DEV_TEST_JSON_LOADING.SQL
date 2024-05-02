
ls @DEV_DB_VISEO.SAP_TABLE_SCHEMA.SAP_JSON_SCHEMAS;

select $1
from @DEV_DB_VISEO.SAP_TABLE_SCHEMA.SAP_JSON_SCHEMAS
(file_format => ff_json_logs);


create or replace file format FF_JSON_LOGS
TYPE = 'JSON',
STRIP_OUTER_ARRAY = True
COMPRESSION = 'AUTO';

SELECT $1:mode::STRING as mode,
           $1:name::STRING as name,
           $1:type::STRING as type
    FROM @DEV_DB_VISEO.SAP_TABLE_SCHEMA.SAP_JSON_SCHEMAS/adr6.schema.json
    (file_format => ff_json_logs);

