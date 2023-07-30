USE <>;
/*
    Build a view ontop of the HL7 data
*/

CREATE OR REPLACE TABLE ge_ris.hl7
(
    message variant,
    filename string,
    importTime datetime default current_timestamp()

);

/*
    Create a file format delimeter object
*/
ALTER FILE FORMAT "ANDOR"."PUBLIC".HL7 
    SET COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = 'NONE' 
    RECORD_DELIMITER = 'MSH' 
    SKIP_HEADER = 0 
    FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('');

/*
    Load data looking at text files with .txt extentions
*/
COPY INTO ge_ris.hl7(message,filename)
FROM
(
    select 
        s.$1::VARIANT
        ,metadata$filename
    FROM @ATHENA.OHDATAMIGRATIONADL/GE_RIS/ s
)
file_format=andor.PUBLIC.HL7
pattern = '.*[.]txt'
;


CREATE OR REPLACE VIEW ge_ris.hl7_vw as
select 
    regexp_substr(split(split(message,'MSH')[0],'|')[2],'[A-Z]{3}') sendingapplication,
    regexp_substr(split(split(message,'PID')[1],'|')[18],'[0-9]{2,8}') mrn, 
    regexp_substr(split(split(message,'PID')[1],'|')[3],'[0-9]{4,20}') alternatemrn, 
    trim(replace(split(message,'|') [8],'^',''),'"') messageType,
    trim(split(message,'|') [6],'"') date,
    trim(split(trim(replace(split(message,'|') [16],'^',' ')),' ')[0],'"') firstname,
    trim(split(trim(replace(split(message,'|') [16],'^',' ')),' ')[1],'"') lastname,
    trim(split(trim(replace(split(message,'|') [16],'^',' ')),' ')[2],'"') middle,
    date(trim(split(trim(split(message,'PID')[1]),'|')[7],'"'),'yyyymmdd')::date dob,
    regexp_substr(trim(split(trim(split(message,'PID')[1]),'|')[19],'"'),'[0-9]{3}-[0-9]{2}-[0-9]{4}') ssn,
    replace(trim(split(trim(split(message,'PID')[1]),'|')[26]),'^',' ') doctor,
    regexp_substr(split(split(message,'PV1')[1],'|')[2] ,'[A-Z]') as bedstatus,    
    regexp_substr(split(trim(message,'"'),'OBX')[3],'[0-9]{2}/[0-9]{2}/[0-9]{4}') procedureAckDate,
    as_array(split(trim(message,'"'),'OBX')) observations,
    f.index,
    rtrim(replace(replace(trim(f.value,'"'),'|',' '),'\r',''),'F') observation,
    filename sourceFile,
    importTime
FROM ge_ris.hl7,
    table (flatten(select as_array(split(trim(message,'"'),'OBX')))) f
;

-- sanity check
select *
FROM  ge_ris.hl7_vw
limit 100;