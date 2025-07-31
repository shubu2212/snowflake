CREATE OR REPLACE PROCEDURE STAGING.SP_LOAD_STG_DATA_RTNTN_DSPSL_LOG(

        s3_file_pattern varchar,

        stg_table varchar

 

)

    returns boolean

    language sql execute

    as caller

    AS

$$

    /****************************************************************************************************************************************

      NAME:      SP_LOAD_STG_DATA_RTNTN_DSPSL_LOG

      PURPOSE:   Procedure to load files in Snowflake from S3 for disposal logs.

      PARAMETER: S3_FILE_PATTERN - Pattern to search files in S3

                 STG_TABLE - Staging table name

   

      Version    Date        Author          Description

      ---------  ----------  --------------  ------------------------------------------------------------------------------------------------

      1.0        2024-09-18 Shubham Agarwal     Initial version

    *****************************************************************************************************************************************/

    DECLARE

    

    v_query_id string;

    v_query string;            -- Variable to create dynamic queries

    v_count number;

    v_string string;             

    v_run_id number               := -1;               -- run not required

 

    -- Variable to create S3 file path by using EXT_STAGE, EXT_STAGE_SUB_DIR, FILE_NAME

    v_src_file_dir string           := '@STAGING.s3_stage/LOGS/';

    v_tgt_table string              := 'STAGING.' || :stg_table;

    v_file_format string            := 'STAGING.S3_FILE_FORMAT';

    v_file_pattern string           := '.*' || :s3_file_pattern ||'.*';

    v_load_start_ts string          := ( select current_timestamp());

    v_user_id string                := ( select current_user());

    v_user_role string              := ( select current_role());

    v_domain string                 := 'DATA RETENTION';

    v_vwh string                    := (select current_warehouse());

    v_sub_domain string             := 'Dataretention disposal log';

    v_comment string                := 'Procedure to load data from AWS S3 bucket into Snowflake table for Disposal Logs files.';

    v_load_name string              := 'SP_LOAD_STG_DATA_RTNTN_DSPSL_LOG';

    v_load_status_in_progress string := 'IN PROGRESS';

    v_load_status_failed string     := 'FAILED';

    v_load_status_success string    := 'SUCCEEDED';

   

    v_file_name string;

    v_rows_parsed number;

    v_rows_loaded number;

    v_file_status string;

    v_errors_seen number;

    v_first_error string;

 

    v_success_payload varchar;

    v_error_payload varchar;

 

    v_all_files array       := [];

    v_all_rows_parsed array := [];

    v_all_rows_loaded array := [];

    v_all_errors_seen array := [];

    v_all_file_status array := [];

    v_all_error_details array := [];

   

    --res RESULTSET DEFAULT (SELECT "file","status","rows_parsed","rows_loaded" FROM dual);

    --c1_cursor cursor for res;

 

    v_inprogress_payload varchar           := (SELECT OBJECT_CONSTRUCT(

                                                'domain' , :v_domain,

                                                'sub_domain' , :v_sub_domain,

                                                'process_start_ts' , :v_load_start_ts,

                                                'load_comment', :v_comment,

                                                'compute', :v_vwh,

                                                'role',:v_user_role));   

 

BEGIN

    -- Inserting in progress in the log table

    call {{CNTL_DB}}.process_control.sp_process_load_log(

                                            :v_run_id,

                                            :v_load_name,

                                            :v_load_start_ts,

                                            :v_load_status_in_progress,

                                            :v_inprogress_payload);

   

    -- Deleting data from staging table before loading new data

    v_query := 'delete from ' || :v_tgt_table || ';';

    execute immediate :v_query;

   

    -- Query to list files available in the stage

    v_query := 'list @STAGING.S3_STAGE/LOGS/';-- PATTERN = \'' || :v_file_pattern ||  '\';'; --pattern = '.*Policy_Disposal.*';;';

    execute immediate :v_query;

 

    -- Loop to validate each file

    Let v_files_cursor CURSOR FOR (SELECT "name" FROM table(result_scan(last_query_id())));

    FOR v_file_row IN v_files_cursor DO

        -- You can access the file name using v_file_row."name"

        v_file_name := split(v_file_row."name",'/')[4];

        -- Check if the file name matches the pattern

        if (RLIKE(:v_file_name, :v_file_pattern, 'i')) then

           

            -- Check if the file has all the required columns as per the staging table

            -- Perform your validation logic to check if the file has all the required columns as per the staging table

            call STAGING.SP_VALIDATE_S3_FILE_STRUCUTRE(:v_file_name, :stg_table) into :v_count;

 

            -- If v_count = 0 then file structure is correct

            if (:v_count = 0) then

                -- Load the file into Snowflake

               

                v_query := 'copy into ' || :v_tgt_table || '

                            from @STAGING.S3_STAGE/LOGS/

                            file_format = (format_name = STAGING.S3_FILE_FORMAT)

                            files = (\'' || :v_file_name || '\'  )

                            on_error = SKIP_FILE

                            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                            INCLUDE_METADATA = (SYS_REC_SRC = METADATA$FILENAME);';

                execute immediate :v_query;

 

                -- Get the status of the file

                select "status","rows_parsed","rows_loaded","errors_seen","first_error"

                    into  :v_file_status, :v_rows_parsed, :v_rows_loaded, :v_errors_seen, :v_first_error

                    from table(result_scan(last_query_id()));

               

                v_all_files := array_append(:v_all_files, :v_file_name);

                v_all_rows_parsed := array_append(:v_all_rows_parsed, :v_rows_parsed);

                v_all_rows_loaded := array_append(:v_all_rows_loaded, :v_rows_loaded);

                v_all_errors_seen := array_append(:v_all_errors_seen, :v_errors_seen);

                v_all_file_status := array_append(:v_all_file_status, :v_file_status);

                v_all_error_details := array_append(:v_all_error_details, ifnull(:v_first_error,'No Error'));  

 

               

                if (:v_file_status = 'LOADED') then

                    -- Copy the file to archive bucket               

                    v_query := concat('COPY FILES INTO @STAGING.S3_STAGE/ARCHIVE/  FROM @STAGING.S3_STAGE/  FILES = (\'LOGS/',:v_file_name,'\');');

                    execute immediate :v_query;

               

                    

                    v_query := 'update ' || :v_tgt_table || '

                            set SYS_CRTN_DT = current_timestamp(),

                                SYS_CRTN_USER_ID = current_user()

                            where SYS_REC_SRC like ''%' || :v_file_name  || ''';';

                    execute immediate :v_query;

                   

                else

                    -- Copy the file to reject bucket

                    v_query := concat('COPY FILES INTO @STAGING.S3_STAGE/REJECT/  FROM @STAGING.S3_STAGE/  FILES = (\'LOGS/',:v_file_name,'\');');

                    execute immediate :v_query;

                end if;

            -- Else copy the file to reject bucket because file structure is incorrect

            else

                -- Copy directly to reject bucket

                v_all_files := array_append(:v_all_files, v_file_name);

                v_all_rows_parsed := array_append(:v_all_rows_parsed, 0);

                v_all_rows_loaded := array_append(:v_all_rows_loaded, 0);

                v_all_errors_seen := array_append(:v_all_errors_seen, 0);

                v_all_file_status := array_append(:v_all_file_status, 'REJECTED');

                v_all_error_details := array_append(:v_all_error_details, 'File structure is incorrect');

 

                v_query := concat('COPY FILES INTO @STAGING.S3_STAGE/REJECT/  FROM @STAGING.S3_STAGE/  FILES = (\'LOGS/',:v_file_name,'\');');

                execute immediate :v_query;

            end if;

            -- Remove the file after file is moved to archive and reject bucket

            v_query := 'remove @STAGING.S3_STAGE/LOGS/' || :v_file_name || ';';

            execute immediate :v_query;

        end if;

    END FOR;

 

   

    -- Inserting success status in log table and creating the success payload

    v_success_payload := (SELECT OBJECT_CONSTRUCT(

                                                    'domain' , :v_domain,

                                                    'sub_domain' , :v_sub_domain,

                                                    'process_start_ts' , :v_load_start_ts,

                                                    'process_end_ts', current_timestamp(),

                                                    'load_comment', :v_comment,

                                                    'load_name', :v_load_name,

                                                    'compute', :v_vwh,

                                                    'role', :v_user_role,

                                                    'query_id', :v_query_id,

                                                    'source', :v_src_file_dir,

                                                    'target', :v_tgt_table,

                                                    'file_name', :v_all_files,

                                                    'file_status', :v_all_file_status,

                                                    'error_details', :v_all_error_details,

                                                    'rows_parsed', :v_all_rows_parsed,

                                                    'rows_loaded', :v_all_rows_loaded,

                                                    'error_records', :v_all_errors_seen

                                                ));

    call {{CNTL_DB}}.process_control.sp_process_load_log(:v_run_id,

                                            :v_load_name,

                                            :v_load_start_ts,

                                            :v_load_status_success,

                                            :v_success_payload);

    return TRUE;

    EXCEPTION   

    WHEN OTHER

    THEN

    BEGIN

    -- Inserting failed status in log table and creating the error payload

            v_error_payload := (SELECT OBJECT_CONSTRUCT('domain' , :v_domain,

                                                'sub_domain' , :v_sub_domain,

                                                'process_start_ts' , :v_load_start_ts,

                                                'process_end_ts', current_timestamp(),

                                                'load_comment', :v_comment,

                                                'load_name', :v_load_name,

                                                'compute', :v_vwh,

                                                                                                                                                                                                'role', :v_user_role,

                                                'source', :v_src_file_dir,

                                                'target', :v_tgt_table,

                                                'file_name', :v_file_name,

                                                'file_status', :v_file_status,

                                                'error_code', :SQLCODE,

                                                'error_msg', :SQLERRM,

                                                'error_state',:SQLSTATE,

                                                'query_id',:v_query_id

                                                ));

            call {{CNTL_DB}}.process_control.sp_process_load_log(:v_run_id,

                                                        :v_load_name,

                                                        :v_load_start_ts,

                                                        :v_load_status_failed,

                                                        :v_error_payload);

            return FALSE;

  

    END;

END;

$$

;
