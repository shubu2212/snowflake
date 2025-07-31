CREATE OR REPLACE PROCEDURE staging.SP_VALIDATE_S3_FILE_STRUCUTRE(file_name VARCHAR, table_name VARCHAR)

RETURNS INT

LANGUAGE SQL

EXECUTE AS CALLER

AS

$$

    /****************************************************************************************************************************************

      NAME:      SP_VALIDATE_S3_FILE_STRUCUTRE

      PURPOSE:   Procedure to validate the file strucutre by passing the file name and staging table name.

      PARAMETER: FILE_NAME - Name of the file in the DATA RETENTION LOG bucket

                 TABLE_NAME - Staging table name

   

      Version    Date        Author          Description

      ---------  ----------  --------------  ------------------------------------------------------------------------------------------------

      1.0        2024-09-18 Shubham Agarwal     Initial version

    *****************************************************************************************************************************************/

DECLARE

    v_result INT;

    v_file_name VARCHAR := file_name;

    v_stg_table VARCHAR := table_name;

    v_query string := 'select count(1) as cnt

                    from information_schema.columns t left outer join

                        (SELECT column_name

                            FROM TABLE(

                                INFER_SCHEMA(

                                LOCATION=>''@STAGING.s3_stage/LOGS/'',

                                FILE_FORMAT =>''STAGING.S3_FILE_FORMAT'' ,

                                IGNORE_CASE => TRUE,

                                FILES => ''' || :v_file_name || '''

                                ) )) s on upper(s.column_name) = t.column_name

                    where t.table_name=''' || :v_stg_table || '''

                    and t.table_schema = ''STAGING'' and t.column_name not like ''SYS_%''

                    and (t.column_name <> s.column_name or s.column_name is null )

                    order by t.ordinal_position;';

   

BEGIN

    EXECUTE IMMEDIATE :v_query ;

        select cnt into :v_result from table(result_scan(last_query_id()));

        RETURN v_result;

EXCEPTION

    WHEN OTHER

    THEN

    BEGIN

            RETURN -1;

    END;

END;

$$;
