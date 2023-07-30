/*
    Snowflake ETL code for a healthcare client.
    Sensitive data removed.
    This is likely raw code

    The nature of this data was very-very deeply nested xml. 
    Used meta-data from the datalake to ensure data lineage was tracked.
    Used regex when I could.

*/


use <>;
use warehouse <>;

create or replace temporary table image_model
(
    firstName      string,
    lastName       string,
    dob            date,
    mrn            string,
    sourceSystems  string,
    path           string
);



select *
FROM image_model;

copy into image_model(firstname, lastname,dob,mrn, sourceSystems, path) 
FROM (select 
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[5],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"','') firstname, --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[5],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"','') lastname, --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy') dob, -- dob
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_','')mrn, -- mrn
      'StCloud Athena Clinical Chart' sourcesystems,
      METADATA$FILENAME path -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_A s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';
      
      copy into image_model(mrn,firstname, lastname,dob, importPath) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_B s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';
      
            copy into image_model(mrn,firstname, lastname,dob, importPath) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_C s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';
      
copy into image_model(mrn,firstname, lastname,dob, importPath) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_D s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';


--use stcloud;
use bayfront;

CREATE OR REPLACE TABLE ATHENA.PDF
(
    mrn int,
    firstname string,
    lastname string,
    dob date,
    path string,
    importtime datetime default current_timestamp(),
    sourcesystem string
);


create or replace temporary table temp1
(
     mrn string
    ,firstname string
    ,lastname string
    ,dob string
    ,path string
    ,importTime datetime default current_timestamp()
);


copy into temp1 (mrn
                ,firstname
                ,lastname
                ,dob
                ,path)
FROM
(
    select distinct
         regexp_substr(metadata$filename,'[0-9]+[.]pdf') as mrn
        ,regexp_substr(metadata$filename,'[.]zip/[A-Z.?]{1,20}-[A-Z]-?[A-Z]{1,20}?') as firstname
        ,regexp_substr(metadata$filename,'[.]zip/[A-Z.?]{1,20}') as  lastname
        ,date(regexp_substr(metadata$filename,'[0-9]{2}-[0-9]{2}-[0-9]{4}'),'mm-dd-yyyy') as  dob
        ,metadata$filename as path
         FROM @STCLOUD.ATHENA.<blank>/preprocessing/Bayfront/Athena/drive1/PDF/
   
)
pattern='.*/.*[.]pdf';



copy into temp1 (mrn
                ,firstname
                ,lastname
                ,dob
                ,path)
FROM
(
    select distinct
         regexp_substr(metadata$filename,'[0-9]+[.]pdf') as mrn
        ,regexp_substr(metadata$filename,'[.]zip/[A-Z.?]{1,20}-[A-Z]-?[A-Z]{1,20}?') as firstname
        ,regexp_substr(metadata$filename,'[.]zip/[A-Z.?]{1,20}') as  lastname
        ,date(regexp_substr(metadata$filename,'[0-9]{2}-[0-9]{2}-[0-9]{4}'),'mm-dd-yyyy') as  dob
        ,metadata$filename as path
         FROM @STCLOUD.ATHENA.<blank>/preprocessing/Bayfront/Athena/drive3/
   
)
pattern='.*/.*[.]pdf';


select distinct 
    replace(regexp_substr(mrn,'[0-9]+'),'.pdf','')::int mrn
   ,trim(split(replace(firstname,'.zip',''),'-')[1],'"') firstname
   ,ltrim(lastname,'.zip/') lastname
   ,dob::date dob
   ,path
   ,importTime
   , 'Bayfront Athena - Clinical Charts' as sourcesystem
FROM temp1;


insert into ATHENA.PDF
(
    select distinct 
    replace(regexp_substr(mrn,'[0-9]+'),'.pdf','')::int mrn
   ,trim(split(replace(firstname,'.zip',''),'-')[1],'"') firstname
   ,ltrim(lastname,'.zip/') lastname
   ,dob::date dob
   ,path
   ,importTime
   ,'Bayfront Athena - Clinical Charts' as sourcesystem 
FROM temp1
);

-- create consolidated view in OH.ATHENA of all PDF's

select *
FROM <blank>.ATHENA.PDF
union 
select *
FROM STCLOUD.ATHENA.PDF;


CREATE OR REPLACE VIEW OH.ATHENA.ALL_PDF
AS
(
    select *
    FROM <blank>.ATHENA.PDF
    union 
    select *
    FROM STCLOUD.ATHENA.PDF
);

SELECT * FROM OH.ATHENA.ALL_PDF;


CREATE TABLE OH.ATHENA.ALL_CCDA
AS
(
    SELECT *
    FROM OH.PUBLIC.ALL_CCDA
)
;

SELECT *
FROM <blank>.ATHENA.HTML_PATIENT_IMAGES_VW
LIMIT 100;


  


use stcloud;


select *
FROM athena.pdf
limit 100;

copy into stcloud.athena.pdf(
                            mrn,
                            firstname,
                            lastname,
                            dob,
                            path,
                            importTime
        
                        )
FROM (  select 
        regexp_substr(METADATA$FILENAME,''),
       'St.Cloud',
        $1,
        METADATA$FILENAME -- import path
        FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/drive1/PDF/ s)
        File_format = <blank>.CLINICALS.CCDAXML
        ON_ERROR = 'continue'
        ;

select count(*) FROM athena.pdf;


select * FROM athena.pdf


        ___________________________

use stcloud;
use warehouse orlandohealth;



CREATE OR REPLACE TABLE PHILIPS_IECG.hl7
(
    message variant,
    filename string,
    importTime datetime default current_timestamp()

);
--truncate table PHILIPS_IECG.hl7;


ALTER FILE FORMAT "ANDOR"."PUBLIC".HL7 
SET COMPRESSION = 'AUTO' RECORD_DELIMITE= 'MSH' 
FIELD_DELIMITER='NONE' SKIP_HEADER = 0 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE ESCAPE = 'NONE'  
DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' 
;

copy into PHILIPS_IECG.hl7(message,filename)
FROM
(
    select 
        s.$1::VARIANT
        ,metadata$filename
    FROM @ATHENA.<blank>/landing/St.Cloud/Philips_iECG/ s
)
file_format=andor.PUBLIC.HL7
ON_ERROR=CONTINUE
pattern = '.*[.]dat'
;


select count(*) 
FROM PHILIPS_IECG.hl7;

CREATE OR REPLACE VIEW PHILIPS_IECG.hl7_vw as
select --split(message,'|') --
    replace(split(message,'PID') [0],'"','') mrn, 
    trim(replace(split(message,'|') [8],'^',''),'"') messageType,
    trim(split(message,'|') [6],'"') date,
   -- trim(split(trim(replace(split(message,'|') [16],'^',' ')),' ')[0],'"') firstname,
   -- trim(split(trim(replace(split(message,'|') [16],'^',' ')),' ')[1],'"') lastname,
   -- trim(split(trim(replace(split(message,'|') [16],'^',' ')),' ')[2],'"') middle,
   -- date(trim(split(trim(split(message,'PID')[1]),'|')[7],'"'),'yyyymmdd')::date dob,
   -- regexp_substr(trim(split(trim(split(message,'PID')[1]),'|')[19],'"'),'[0-9]{3}-[0-9]{2}-[0-9]{3}') ssn,
    --replace(trim(split(trim(split(message,'PID')[1]),'|')[26]),'^',' ') doctor
    --as_array(split(trim(message,'"'),'OBX')) observations,
    f.index,
    rtrim(replace(replace(trim(f.value,'"'),'|',' '),'\r',''),'F') observation,
    filename sourceFile,
    importTime
FROM PHILIPS_IECG.hl7,
    table (flatten(select as_array(split(trim(message,'"'),'OBX')))) f
  limit 10000
   ;
   
   
-- Evaluate the type of HL7 we are processing
truncate table PHILIPS_IECG.hl7;
copy into PHILIPS_IECG.hl7(message,filename)
FROM
(
    select 
         s.$1::VARIANT
        ,metadata$filename
    FROM @ATHENA.<blank>/landing/St.Cloud/Philips_iECG/ s
)
file_format=andor.PUBLIC.HL7
pattern = '.*[.]dat'
;


select * FROM PHILIPS_IECG.hl7_vw





use stcloud;
use warehouse orlandohealth;

select
'Athena' as source_system
,'St.Cloud - Athena' as source_system_name
, null as source_system_patient_id
,"Mrn" as patient_id_1
,'MRN' as patientid_id_type
,null as patient_id_2
,null as patient_id_2_type
,null as patient_id_3
,null as patient_id_3_type
,"PatientFirstName" as patient_first_name
,"PatientMiddleName" as patient_middle_name
,"PatientLastName" as patient_last_name
,"Gender" as patient_sex
,"PatientDob" as patient_dob
,null as patient_deceased_date
,null as patient_active
,null as patient_confidential
,"Id" as source_system_encounter_id
,null as header_encounter_id
,null as enc_account_number
,null as enc_confidential
,null as enc_status
,null as enc_region
,'St.Cloud' as enc_facility
--,"PerformerType" as enc_facility
,"AssignedEntityAddr" as enc_location
, null as enc_room
, null as enc_bed
, "EffectiveTime" as enc_service_date
, "ServiceEventStartTime" as enc_admit_date
, "ServiceEventEndTime" as enc_discharge_date
, null as enc_admit_doctor
, "Provider" as enc_attend_doctor
, null as enc_primary_doctor
, null as enc_family_doctor
, "EncounterReason" as enc_reason_for_visit
, null as enc_additional_properties
, null as report_id
, 'FALSE' as rpt_confidential
, null as group_name
, "FileName" as report_name
, null as rpt_file_location
, null as rpt_orientation
, "ImportTime" as rpt_document_date
, null as rpt_author
, "DocumentType" as rpt_document_type
, null as rpt_additional_properties
FROM athena.encounters
limit 100;





select  a.mrn
       ,case
          when a.firstname is null then
              upper(b.firstname)
          else upper(a.firstname)
          end as firstname
       ,case
          when a.lastname is null then
              upper(b.lastname)
          else upper(a.lastname)
        end as lastname
       ,case
        when a.dob is null then
            upper(b.dob)
        else upper(a.dob)
        end as dob
       ,upper(a.ssn)ssn
       ,upper(a.sex)sex
       ,upper(a.address)address
       ,upper(a.city)city
       ,upper(a.state)state
       ,upper(a.zip)zip
       ,upper(a.homephone)homephone
       ,upper(a.sourcesystem)sourcesystem
FROM "STCLOUD"."ATHENA"."CCDA_PATIENTS" a
left join "STCLOUD"."ATHENA"."PDF" b
on a.mrn = b.mrn
and soundex(a.firstname) = soundex(b.firstname)
and soundex(a.lastname) = soundex(b.lastname)
and soundex(a.dob) = soundex(b.dob)





use stcloud;


select * FROM stcloud.SUNRISE.COMBINEDCCDA;

create transient table oh.public.all_ccda
as
(
    select mrn::int
       ,documents
       ,case 
          when path like '%drive%'
          then 'Bayfront'
        else 'St.Cloud'
       end as institution 
       ,path
FROM stcloud.athena.ccda
union
select regexp_substr(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'id'):"@extension",'[0-9]+')::int as mrn
        ,documents
        ,'St.Cloud ' as institution 
        ,path
FROM  stcloud.pulse.ccda
union
select  mrn
        ,document
        ,'St.Cloud' as institution
        ,path
FROM stcloud.SUNRISE.COMBINEDCCDA



);

desc table stcloud.SUNRISE.COMBINEDCCDA;

select  mrn
        ,document
        ,'St.Cloud' as institution
        ,path
FROM stcloud.SUNRISE.COMBINEDCCDA
limit 10;


use stcloud;
use warehouse ORLANDOHEALTH;

create or replace temporary table image_model
(
    firstName      string,
    lastName       string,
    dob            date,
    mrn            string,
    sourceSystems  string,
    path           string
);



select *
FROM image_model;

copy into image_model(firstname, lastname,dob,mrn, sourceSystems, path) 
FROM (select 
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[5],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"','') firstname, --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[5],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"','') lastname, --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy') dob, -- dob
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_','')mrn, -- mrn
      'StCloud Athena Clinical Chart' sourcesystems,
      METADATA$FILENAME path -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_A s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';
      
      copy into image_model(mrn,firstname, lastname,dob, importPath) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_B s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';
      
            copy into image_model(mrn,firstname, lastname,dob, importPath) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_C s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';
      
copy into image_model(mrn,firstname, lastname,dob, importPath) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_D s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';






use stcloud;

copy into stage_patientModel(mrn,firstname, lastname,dob,path,sourceSystems,sourceDataType) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME as path, -- import path
      'HTML' as sourceDataType,
      'Athena Clinical Chart' as sourceSystems
      FROM @ATHENA.<blank>/preprocessing/Bayfront/Athena/drive1/html/ClinicalCharts_PG_26-261_R s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/.*/.*[a-z | A-Z]{1,20}[.]png|.*/.*/.*[a-z | A-Z]{1,20}[.]gif';
      
      select * FROM stage_patientModel;
      
desc table stage_patientmodel;
      
select firstname 
FROM stage_patientModel
order by 1 desc




copy into stage_patientModel(mrn,firstname, lastname,dob,path,sourceSystems,sourceDataType) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME as path, -- import path
      'HTML' as sourceDataType,
      'Athena Clinical Chart' as sourceSystems
      FROM @ATHENA.<blank>/preprocessing/Bayfront/Athena/drive2/html/ClinicalCharts_PG_81-101-121-141-241-281_A s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/.*/.*[a-z | A-Z]{1,20}[.]png|.*/.*/.*[a-z | A-Z]{1,20}[.]gif';
      
    ;
    
    select *
    FROM bayfront.athena.stage_html_patient_model;






use andor;
use warehouse andoredw;

CREATE OR REPLACE TABLE master.patients
(
    ExternalOrganizationId VARCHAR(255)
    ,Mrn VARCHAR(36) 
    ,FirstName VARCHAR(30)
    ,LastName VARCHAR(30)
    ,MiddleName VARCHAR(20) 
    ,Prefix VARCHAR(5) 
    ,Suffix VARCHAR(5) 
    ,BirthDate DATETIME 
    ,GenderCode CHAR(1) 
    ,Ssn VARCHAR(11) 
    ,MaritalStatus VARCHAR(36) 
    ,StreetAddress1 VARCHAR(50) 
    ,StreetAddress2 VARCHAR(50) 
    ,CityName VARCHAR(40) 
    ,StateCode VARCHAR(40) 
    ,ZipCode VARCHAR(40) 
    ,CountyName VARCHAR(50) 
    ,CountryCode VARCHAR(50) 
    ,Alias VARCHAR(255) 
    ,HomePhone VARCHAR(20) 
    ,CellPhone VARCHAR(20) 
    ,WorkPhone VARCHAR(20) 
    ,EmailAddress VARCHAR(255) 
    ,LanguageCode VARCHAR(255) 
    ,PrimaryLanguage VARCHAR(100) 
    ,RaceCode VARCHAR(255)  
    ,ReligionCode VARCHAR(36) 
    ,EthnicGroupCode VARCHAR(36) 
    ,FacilityExternalId VARCHAR(50) 
    ,FacilityUnitExternalId VARCHAR(50) 
    ,AlternativeExternalId VARCHAR(50) 
    ,PcpExternalId VARCHAR(50) 
    ,PcpLastName VARCHAR(50) 
    ,PcpFirstName VARCHAR(50) 
    ,PcpMiddleName VARCHAR(50) 
    ,VipStatusCode VARCHAR(36) 
    ,Deceased int
    ,DeceasedDateTime datetime
    ,InputSource VARCHAR(30)
    ,ImportTime datetime default current_timestamp()
    ,isvalid int
    ,invalidatedAt datetime
    ,invalidatedBy varchar(25)
);


create schema landing;

create or replace transient table landing.mirth_hl7
(
    message variant,
    importTime datetime default current_timestamp(),
    importedBy string,
    isValid int,
    invalidatedBy string
);


CREATE OR REPLACE TABLE landing.ccda_patients
(
    message string,
    importTime datetime default current_timestamp(),
    importedBy string,
    isValid int,
    invalidatedBy string
    
);


create or replace stream patients_hl7_stream on table landing.hl7_patients;

create or replace stream patients_xml_stream  on table landing.ccda_patients;


insert into landing.hl7_patients (message,importTime,importedBy,isValid,invalidatedBy) values
(
   '{                                    
   "id": 7077,                        
   "x1": "2018-08-14T20:57:01-07:00", 
   "x2": [                            
     {                                
       "y1": "green",                 
       "y2": "35"                     
     }                                
   ]                                  
 }'
 ,
  current_timestamp(),'a',1,'b'
  
);


insert into landing.ccda_patients (message,importTime,importedBy,isValid,invalidatedBy) values
(
   '{                                    
   "id": 7077,                        
   "x1": "2018-08-14T20:57:01-07:00", 
   "x2": [                            
     {                                
       "y1": "green",                 
       "y2": "35"                     
     }                                
   ]                                  
 }'
 ,
  current_timestamp(),'a',1,'b'
  
);

select h.message,x.message
FROM patients_hl7_stream h, patients_xml_stream x



CREATE OR REPLACE TABLE pulse.medications
(
     AccountNumber string,
     MRN int,
     AdmitDate date,
     DischargeDate date,
     FinancialClass string,
     AdmitFinancialClass string,
     DischargeFinancialClass string,
     AdmissionType string,
     AdmissionSource string,
     PatientType string,
     HospitalServiceCode string,
     PhysianNumber string,
     AdmitHospitalServiceCode string
);


copy into stcloud.pulse.medications
FROM 
(
     select 
     $1:AccountNumber,
     $1:MRN ,
     $1:AdmitDate date,
     $1:DischargeDate date,
     $1:FinancialClass ,
     $1:AdmitFinancialClass ,
     $1:DischargeFinancialClass ,
     $1:AdmissionType ,
     $1:AdmissionSource ,
     $1:PatientType ,
     $1:HospitalServiceCode ,
     $1:PhysianNumber ,
     $1:AdmitHospitalServiceCode
     FROM @ATHENA.<blank>/preprocessing/StCloud/Pulse/medications/Medications.parquet/medications.snappy.parquet s
)
File_format= andor.master.PARQUETFILETYPE


select count(*)FROM stcloud.pulse.medications




use stcloud;
use schema athena;
use role accountadmin;
-- re-create the ccda table and re-make into a transient table
grant select on all tables on schema MY_DB.MY_SCHEMA to role TEST_ROLE;

grant select on all tables in schema stcloud.athena to role public;


grant select on all objects in schema athena to public;

show views;

select * 
FROM athena.ccda_vw;

CREATE OR REPLACE VIEW STCLOUD.ATHENA.CCDA_VW
as
(
    select 
    
    trim(split(xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'given'):"$",' ')[0],'"') as firstname
    ,trim(split(xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'family'):"$",' ')[0],'"') as lastname 
    ,trim(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'id'):"@extension",'"') mrn
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'streetAddressLine'):"$",'"') address
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'city'):"$",'"') city
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'state'):"$",'"') state
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'postalCode'):"$",'"') postalCode
    --,trim(xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'given'):"$",'"') firstName
    --,trim(xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'family'):"$",'"') lastName
    ,REGEXP_SUBSTR(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'birthTime'):"@value",'([0-9]+{4}[0-9]+{2}[0-9]+{2})') dob
    ,replace(REGEXP_SUBSTR(trim(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'telecom'):"@value",'"'),'[0-9]{3}-[0-9]{7}'),'-','') homephone
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'administrativeGenderCode'):"@code",'"') gender
    ,trim(xmlget(documents,'effectiveTime'):"@value",'"') as effectiveTime
    --,documents
    ,case when lower(path) like '%drive%' then
            'Bayfront - Athena CCDA'
         else 'St.Cloud'
        end as sourceSystem
    ,path
    ,importtime
FROM  stcloud.athena.ccda
 
);



create or replace transient table public.stage_ccda
(
    documents variant,
    path string,
    importTime timestamp default current_timestamp()
)


--landing/St.Cloud/Athena/Athena_Drop/CCDA/CCDA_Unformatted/
copy into public.stage_ccda(
                documents,
                path 

            )
FROM (select 
            athena.$1,
            METADATA$FILENAME -- import path
            FROM @ATHENA.<blank>/landing/St.Cloud/Athena/Athena_Drop/CCDA/CCDA_Unformatted/ athena
      )
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE;
     

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    pulse.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/Pulse/CCDAfIleDrop/ pulse
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE

;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/Sunrise_CCDA_Extract/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE

;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_01_28_2021/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE

;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_01_30_2021A/RoundTwo/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE
;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_01_30_2021B/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE
;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_03_17_2021/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE
;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_04_23_2021/Pickup/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE
;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_04_24_2021/04_24_2021/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE
;

copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    sunrise.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/landing/St.Cloud/SunriseCCDA/Archived/Darryl_04_24_2021/Missed_04_23_2021/ sunrise
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*xml'
ON_ERROR = CONTINUE
;

-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_01_28_2021/           --F
-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_01_30_2021A/RoundTwo/ --F
-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_01_30_2021B/          --F
-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_03_17_2021/           --F
-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_04_23_2021/Pickup/    --F
-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_04_24_2021/04_24_2021/ --F
-- landing/St.Cloud/SunriseCCDA/Archived/Darryl_04_24_2021/Missed_04_23_2021/ --F


copy into public.stage_ccda(
            documents,
            path 

    )
FROM(select
    athena.$1,
    METADATA$FILENAME -- import path
    FROM @ATHENA.<blank>/preprocessing/Bayfront/Athena/drive1/ccda/CCDAs athena
)
file_format = <blank>.CLINICALS.CCDAXML
pattern = '.*/.*xml'
ON_ERROR = CONTINUE
;


select count(distinct documents) FROM public.stage_ccda;


select * FROM
public.stage_ccda
limit 100;





select * 
FROM public.stage_ccda
where path like '%Bayfront%' 
limit 100


create or replace transient table oh.public.all_ccda
as
(
    select distinct xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'id'):"@extension"::string as mrn
       ,documents
        ,xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'given'):"$"::string as "FirstName"
        ,xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'family'):"$"::string as "LastName"
        ,to_varchar(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'birthTime'):"@value") as "Dob"
       ,case 
            when path like '%Bayfront%' and path like '%Athena%'
                then 'Bayfront - Athena CCDA'
            when path like '%St.Cloud%' and path like '%Athena%'
                then 'St.Cloud - Athena CCDA'
            when path like '%St.Cloud%' and path like '%Sunrise%'
                then 'St.Cloud - Sunrise CCDA'
            when path like '%St.Cloud%' and path like '%Pulse%'
                        then 'St.Cloud - Pulse CCDA'
            else 'Unknown Source'
        end as source 
       ,path
FROM stcloud.public.stage_ccda
);





select ccda.*
FROM stcloud.public.masterpatientlist p
left join 
(
    select 
    mrn
    ,xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'given'):"$"::string as "FirstName"
    ,xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'family'):"$"::string as "LastName"
    ,to_varchar(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'birthTime'):"@value") as "Dob"
    ,xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'streetAddressLine'):"$"::string as "Address"
    ,xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'city'):"$"::string as "City"
    ,xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'state'):"$"::string as "State"
    ,xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'postalCode'):"$"::string as "Zip"
    ,source
    FROM oh.public.all_ccda
) ccda
on p.mrn = ccda.mrn
where ccda.mrn is null;



select *
FROM oh.public.all_ccda
where UPPER(firstname) = 'BARROWS'
AND UPPER(lastname) = 'LOIS'




use stcloud;
use warehouse orlandohealth;



CREATE OR REPLACE TABLE stcloud.pulse.ccda
(
    mrn int,
    institution string,
    documents variant,
    path string,
    importtime datetime default current_Timestamp()
    
);


copy into stcloud.pulse.ccda(
                            mrn,
                            institution,
                            documents,
                            path
                        )
FROM (select 
            null,
           'St.Cloud',
            $1,
            METADATA$FILENAME -- import path
            FROM @ATHENA.<blank>/landing/St.Cloud/Pulse/CCDAfIleDrop/ s)
            File_format = <blank>.CLINICALS.CCDAXML
            ON_ERROR = 'continue'
            ;
            


CREATE OR REPLACE VIEW stcloud.pulse.ccda_vw
as
(
    select trim(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'id'):"@extension",'"') mrn
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'streetAddressLine'):"$",'"') address
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'city'):"$",'"') city
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'state'):"$",'"') state
    ,trim(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'addr'),'postalCode'):"$",'"') postalCode
    ,trim(xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'given'):"$",'"') firstName
    ,trim(xmlget(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'name'),'family'):"$",'"') lastName
    ,date(xmlget(xmlget(xmlget(xmlget(documents,'recordTarget'),'patientRole'),'patient'),'birthTime'):"@value",'yyyymmdd') dob
    ,documents
    ,institution
    ,path
    ,importtime
FROM  stcloud.pulse.ccda

);


select * FROM stcloud.pulse.ccda_vw;






CREATE OR REPLACE TABLE pulse.masterPatients
(
    PT_PATIENT_COID string,
    PT_PATIENT_NUMBER string,
    PT_PATIENT_NAME string,
    PT_ADMISSION_DATE string,
    PT_ADMISSION_TIME string,
    PT_ADMISSION_TYPE string,
    PT_FINANCIAL_CLASS_CODE string,
    PT_ADMITTING_CLERK string,
    PT_PATIENT_ADDRESS1 string,
    PT_PATIENT_ADDRESS2 string,
    PT_PATIENT_CITY string,
    PT_PATIENT_STATE string,
    PT_PATIENT_ZIP_CODE string,
    PT_PATIENT_COUNTY string,
    PT_PATIENT_COUNTRY string,
    PT_PATIENT_AREA_CODE string,
    PT_PATIENT_TELEPHONE_NO string,
    PT_SOCIAL_SECURITY_NO string,
    PT_BIRTH_DATE string,
    PT_PATIENT_RACE string,
    PT_PATIENT_SEX string,
    PT_PATIENT_RELIGION_CODE string,
    PT_PRIV_CD string,
    PT_PATIENT_EMPLOYER string,
    PT_EMPLOYER_AREA_CODE string,
    PT_EMPLOYER_PHONE_NO string,
    PT_EMPLOYMENT_STAT_CODE string,
    PT_MED_KEY_Y_N string,
    PT_MEDICAL_RECORD_NUMBER string,
    PT_MEDICAL_RECORD_TYPE string,
    PT_VISITING_TYPE string,
    PT_ADMITTING_SOURCE string,
    PT_DIAGNOSIS_DESC string,
    PT_ADMITTING_PHYSICIAN string,
    PT_OTHER_PHYSICIAN string,
    PT_PATIENT_ROOM string,
    PT_PATIENT_BED string,
    PT_GUARANTOR_NAME string,
    PT_GUARANTOR_ADDRESS string,
    PT_GUARANTOR_ADDRESS_2 string,
    PT_GUARANTOR_CITY string,
    PT_GUARANTOR_STATE string,
    PT_GUARANTOR_ZIP string,
    PT_GUARANTOR_COUNTRY string,
    PT_GUARANTOR_AREA_CODE string,
    PT_GUARANTOR_PHONE string,
    PT_GUARANTOR_EMPLOYER string,
    PT_GUARANTOR_EMPL_ADDR string,
    PT_GUARANTOR_EMPL_CITY string,
    PT_GUARANTOR_EMPL_STATE string,
    PT_GUARANTOR_EMPL_ZIP string,
    PT_GUARANTOR_EMPL_AREA_CODE string,
    PT_GUARANTOR_EMPL_PHONE string,
    PT_GUARANTOR_SEX string,
    PT_GUARANTOR_RELATE_TO_PAT string,
    PT_GUARANTOR_OCCUPATION string,
    PT_GUARANTOR_SOC_SEC_NO string,
    PT_ACCIDENT_AUTO string,
    PT_ACCIDENT_AUTO_NOFAULT string,
    PT_ACCIDENT_AUTO_TORT string,
    PT_ACCIDENT_AUTO_EMPLOY string,
    PT_ACCIDENT_AUTO_OTHER string,
    PT_REPORT_LOC string,
    PT_ACCIDENT_TIME string,
    PT_ACCIDENT_LOCATION string,
    PT_FREE_FORM_COMMENT2_2 string,
    PT_NEXT_OF_KIN_NAME string,
    PT_NEXT_OF_KIN_ADDRESS string,
    PT_NEXT_OF_KIN_CITY string,
    PT_NEXT_OF_KIN_STATE string,
    PT_NEXT_OF_KIN_ZIP string,
    PT_NEXT_OF_KIN_AREA_CODE string,
    PT_NEXT_OF_KIN_HOME_PHON string,
    PT_NEXT_OF_KIN_REL string,
    PT_OTHER_RESP_NAME string,
    PT_OTHER_RESP_ADDRESS string,
    PT_OTHER_RESP_CITY string,
    PT_OTHER_RESP_STATE string,
    PT_OTHER_RESP_ZIP string,
    PT_OTHER_RESP_SOC_SEC_NO string,
    PT_FREE_FORM_COMMENT1 string,
    PT_FREE_FORM_COMMENT2 string,
    PT_DISCHARGE_DATE string,
    PT_DISCHARGE_TIME string,
    PT_DISCHARGE_CODE string,
    PT_DISCHARGE_CLERK string,
    PT_OTHER_RESP_AREA_CODE string,
    PT_OTHER_RESP_PHONE string,
    PT_OTHER_RESP_EMPLOYER string,
    PT_OTHER_RESP_EMPL_ADDR string,
    PT_OTHER_RESP_EMPL_CITY string,
    PT_OTHER_RESP_EMPL_STATE string,
    PT_OTHER_RESP_EMPL_ZIP string,
    PT_OTHER_RESP_EMPL_AREA_CODE string,
    PT_OTHER_RESP_EMPL_PHONE string,
    PT_OTHER_RESP_SEX string,
    PT_OTHER_RESP_REL_TO_PAT string,
    PT_OTHER_RESP_OCCUPATION string,
    PT_PROGRAM string,
    PT_OTHER_RESP_DOB string,
    PT_ACCOMMODATION_CODE string,
    PT_PATIENT_TYPE string,
    PT_PATIENT_SUB_TYPE string,
    PT_DISCHARGE_STATUS string,
    PT_GUARANTOR_DOB string,
    PT_PATIENT_AGE string,
    PT_PRIVATE_ROOM string,
    PT_ER_ADMIT string,
    PT_MOTHER_BABY_XREF_1 string,
    PT_MOTHER_BABY_XREF_2 string,
    PT_MOTHER_BABY_COMB string,
    PT_XRAY_NUMBER string,
    PT_REF_DOC_1 string,
    PT_TRANSACTION_CODE string,
    PT_TRANSMISSION_CODE string,
    PT_TRANSMITTAL_DATE string,
    PT_ORIGINATING_TERM_ID string,
    PT_PREV_PATIENT_TYPE string,
    PT_PREV_PATIENT_ROOM string,
    PT_PREV_PATIENT_BED string,
    PT_PRINT_LANGUAGE string,
    PT_MSP_PRINT string,
    PT_PATIENT_HEIGHT string,
    PT_PATIENT_WEIGHT string,
    PT_ALLERGIES_CHECKED string,
    PT_SOUNDEX string
);


copy into pulse.masterPatients
(
    PT_PATIENT_COID,
    PT_PATIENT_NUMBER,
    PT_PATIENT_NAME,
    PT_ADMISSION_DATE,
    PT_ADMISSION_TIME,
    PT_ADMISSION_TYPE,
    PT_FINANCIAL_CLASS_CODE,
    PT_ADMITTING_CLERK,
    PT_PATIENT_ADDRESS1,
    PT_PATIENT_ADDRESS2,
    PT_PATIENT_CITY,
    PT_PATIENT_STATE,
    PT_PATIENT_ZIP_CODE,
    PT_PATIENT_COUNTY,
    PT_PATIENT_COUNTRY,
    PT_PATIENT_AREA_CODE,
    PT_PATIENT_TELEPHONE_NO,
    PT_SOCIAL_SECURITY_NO,
    PT_BIRTH_DATE,
    PT_PATIENT_RACE,
    PT_PATIENT_SEX,
    PT_PATIENT_RELIGION_CODE,
    PT_PRIV_CD,
    PT_PATIENT_EMPLOYER,
    PT_EMPLOYER_AREA_CODE,
    PT_EMPLOYER_PHONE_NO,
    PT_EMPLOYMENT_STAT_CODE,
    PT_MED_KEY_Y_N,
    PT_MEDICAL_RECORD_NUMBER,
    PT_MEDICAL_RECORD_TYPE,
    PT_VISITING_TYPE,
    PT_ADMITTING_SOURCE,
    PT_DIAGNOSIS_DESC,
    PT_ADMITTING_PHYSICIAN,
    PT_OTHER_PHYSICIAN,
    PT_PATIENT_ROOM,
    PT_PATIENT_BED,
    PT_GUARANTOR_NAME,
    PT_GUARANTOR_ADDRESS,
    PT_GUARANTOR_ADDRESS_2,
    PT_GUARANTOR_CITY,
    PT_GUARANTOR_STATE,
    PT_GUARANTOR_ZIP,
    PT_GUARANTOR_COUNTRY,
    PT_GUARANTOR_AREA_CODE,
    PT_GUARANTOR_PHONE,
    PT_GUARANTOR_EMPLOYER,
    PT_GUARANTOR_EMPL_ADDR,
    PT_GUARANTOR_EMPL_CITY,
    PT_GUARANTOR_EMPL_STATE,
    PT_GUARANTOR_EMPL_ZIP,
    PT_GUARANTOR_EMPL_AREA_CODE,
    PT_GUARANTOR_EMPL_PHONE,
    PT_GUARANTOR_SEX,
    PT_GUARANTOR_RELATE_TO_PAT,
    PT_GUARANTOR_OCCUPATION,
    PT_GUARANTOR_SOC_SEC_NO,
    PT_ACCIDENT_AUTO,
    PT_ACCIDENT_AUTO_NOFAULT,
    PT_ACCIDENT_AUTO_TORT,
    PT_ACCIDENT_AUTO_EMPLOY,
    PT_ACCIDENT_AUTO_OTHER,
    PT_REPORT_LOC,
    PT_ACCIDENT_TIME,
    PT_ACCIDENT_LOCATION,
    PT_FREE_FORM_COMMENT2_2,
    PT_NEXT_OF_KIN_NAME,
    PT_NEXT_OF_KIN_ADDRESS,
    PT_NEXT_OF_KIN_CITY,
    PT_NEXT_OF_KIN_STATE,
    PT_NEXT_OF_KIN_ZIP,
    PT_NEXT_OF_KIN_AREA_CODE,
    PT_NEXT_OF_KIN_HOME_PHON,
    PT_NEXT_OF_KIN_REL,
    PT_OTHER_RESP_NAME,
    PT_OTHER_RESP_ADDRESS,
    PT_OTHER_RESP_CITY,
    PT_OTHER_RESP_STATE,
    PT_OTHER_RESP_ZIP,
    PT_OTHER_RESP_SOC_SEC_NO,
    PT_FREE_FORM_COMMENT1,
    PT_FREE_FORM_COMMENT2,
    PT_DISCHARGE_DATE,
    PT_DISCHARGE_TIME,
    PT_DISCHARGE_CODE,
    PT_DISCHARGE_CLERK,
    PT_OTHER_RESP_AREA_CODE,
    PT_OTHER_RESP_PHONE,
    PT_OTHER_RESP_EMPLOYER,
    PT_OTHER_RESP_EMPL_ADDR,
    PT_OTHER_RESP_EMPL_CITY,
    PT_OTHER_RESP_EMPL_STATE,
    PT_OTHER_RESP_EMPL_ZIP,
    PT_OTHER_RESP_EMPL_AREA_CODE,
    PT_OTHER_RESP_EMPL_PHONE,
    PT_OTHER_RESP_SEX,
    PT_OTHER_RESP_REL_TO_PAT,
    PT_OTHER_RESP_OCCUPATION,
    PT_PROGRAM,
    PT_OTHER_RESP_DOB,
    PT_ACCOMMODATION_CODE,
    PT_PATIENT_TYPE,
    PT_PATIENT_SUB_TYPE,
    PT_DISCHARGE_STATUS,
    PT_GUARANTOR_DOB,
    PT_PATIENT_AGE,
    PT_PRIVATE_ROOM,
    PT_ER_ADMIT,
    PT_MOTHER_BABY_XREF_1,
    PT_MOTHER_BABY_XREF_2,
    PT_MOTHER_BABY_COMB,
    PT_XRAY_NUMBER,
    PT_REF_DOC_1,
    PT_TRANSACTION_CODE,
    PT_TRANSMISSION_CODE,
    PT_TRANSMITTAL_DATE,
    PT_ORIGINATING_TERM_ID,
    PT_PREV_PATIENT_TYPE,
    PT_PREV_PATIENT_ROOM,
    PT_PREV_PATIENT_BED,
    PT_PRINT_LANGUAGE,
    PT_MSP_PRINT,
    PT_PATIENT_HEIGHT,
    PT_PATIENT_WEIGHT,
    PT_ALLERGIES_CHECKED,
    PT_SOUNDEX
)
FROM 
    (
        select 
        $1:PT_PATIENT_COID,
        $1:PT_PATIENT_NUMBER,
        $1:PT_PATIENT_NAME,
        $1:PT_ADMISSION_DATE,
        $1:PT_ADMISSION_TIME,
        $1:PT_ADMISSION_TYPE,
        $1:PT_FINANCIAL_CLASS_CODE,
        $1:PT_ADMITTING_CLERK,
        $1:PT_PATIENT_ADDRESS1,
        $1:PT_PATIENT_ADDRESS2,
        $1:PT_PATIENT_CITY,
        $1:PT_PATIENT_STATE,
        $1:PT_PATIENT_ZIP_CODE,
        $1:PT_PATIENT_COUNTY,
        $1:PT_PATIENT_COUNTRY,
        $1:PT_PATIENT_AREA_CODE,
        $1:PT_PATIENT_TELEPHONE_NO,
        $1:PT_SOCIAL_SECURITY_NO,
        $1:PT_BIRTH_DATE,
        $1:PT_PATIENT_RACE,
        $1:PT_PATIENT_SEX,
        $1:PT_PATIENT_RELIGION_CODE,
        $1:PT_PRIV_CD,
        $1:PT_PATIENT_EMPLOYER,
        $1:PT_EMPLOYER_AREA_CODE,
        $1:PT_EMPLOYER_PHONE_NO,
        $1:PT_EMPLOYMENT_STAT_CODE,
        $1:PT_MED_KEY_Y_N,
        $1:PT_MEDICAL_RECORD_NUMBER,
        $1:PT_MEDICAL_RECORD_TYPE,
        $1:PT_VISITING_TYPE,
        $1:PT_ADMITTING_SOURCE,
        $1:PT_DIAGNOSIS_DESC,
        $1:PT_ADMITTING_PHYSICIAN,
        $1:PT_OTHER_PHYSICIAN,
        $1:PT_PATIENT_ROOM,
        $1:PT_PATIENT_BED,
        $1:PT_GUARANTOR_NAME,
        $1:PT_GUARANTOR_ADDRESS,
        $1:PT_GUARANTOR_ADDRESS_2,
        $1:PT_GUARANTOR_CITY,
        $1:PT_GUARANTOR_STATE,
        $1:PT_GUARANTOR_ZIP,
        $1:PT_GUARANTOR_COUNTRY,
        $1:PT_GUARANTOR_AREA_CODE,
        $1:PT_GUARANTOR_PHONE,
        $1:PT_GUARANTOR_EMPLOYER,
        $1:PT_GUARANTOR_EMPL_ADDR,
        $1:PT_GUARANTOR_EMPL_CITY,
        $1:PT_GUARANTOR_EMPL_STATE,
        $1:PT_GUARANTOR_EMPL_ZIP,
        $1:PT_GUARANTOR_EMPL_AREA_CODE,
        $1:PT_GUARANTOR_EMPL_PHONE,
        $1:PT_GUARANTOR_SEX,
        $1:PT_GUARANTOR_RELATE_TO_PAT,
        $1:PT_GUARANTOR_OCCUPATION,
        $1:PT_GUARANTOR_SOC_SEC_NO,
        $1:PT_ACCIDENT_AUTO,
        $1:PT_ACCIDENT_AUTO_NOFAULT,
        $1:PT_ACCIDENT_AUTO_TORT,
        $1:PT_ACCIDENT_AUTO_EMPLOY,
        $1:PT_ACCIDENT_AUTO_OTHER,
        $1:PT_REPORT_LOC,
        $1:PT_ACCIDENT_TIME,
        $1:PT_ACCIDENT_LOCATION,
        $1:PT_FREE_FORM_COMMENT2_2,
        $1:PT_NEXT_OF_KIN_NAME,
        $1:PT_NEXT_OF_KIN_ADDRESS,
        $1:PT_NEXT_OF_KIN_CITY,
        $1:PT_NEXT_OF_KIN_STATE,
        $1:PT_NEXT_OF_KIN_ZIP,
        $1:PT_NEXT_OF_KIN_AREA_CODE,
        $1:PT_NEXT_OF_KIN_HOME_PHON,
        $1:PT_NEXT_OF_KIN_REL,
        $1:PT_OTHER_RESP_NAME,
        $1:PT_OTHER_RESP_ADDRESS,
        $1:PT_OTHER_RESP_CITY,
        $1:PT_OTHER_RESP_STATE,
        $1:PT_OTHER_RESP_ZIP,
        $1:PT_OTHER_RESP_SOC_SEC_NO,
        $1:PT_FREE_FORM_COMMENT1,
        $1:PT_FREE_FORM_COMMENT2,
        $1:PT_DISCHARGE_DATE,
        $1:PT_DISCHARGE_TIME,
        $1:PT_DISCHARGE_CODE,
        $1:PT_DISCHARGE_CLERK,
        $1:PT_OTHER_RESP_AREA_CODE,
        $1:PT_OTHER_RESP_PHONE,
        $1:PT_OTHER_RESP_EMPLOYER,
        $1:PT_OTHER_RESP_EMPL_ADDR,
        $1:PT_OTHER_RESP_EMPL_CITY,
        $1:PT_OTHER_RESP_EMPL_STATE,
        $1:PT_OTHER_RESP_EMPL_ZIP,
        $1:PT_OTHER_RESP_EMPL_AREA_CODE,
        $1:PT_OTHER_RESP_EMPL_PHONE,
        $1:PT_OTHER_RESP_SEX,
        $1:PT_OTHER_RESP_REL_TO_PAT,
        $1:PT_OTHER_RESP_OCCUPATION,
        $1:PT_PROGRAM,
        $1:PT_OTHER_RESP_DOB,
        $1:PT_ACCOMMODATION_CODE,
        $1:PT_PATIENT_TYPE,
        $1:PT_PATIENT_SUB_TYPE,
        $1:PT_DISCHARGE_STATUS,
        $1:PT_GUARANTOR_DOB,
        $1:PT_PATIENT_AGE,
        $1:PT_PRIVATE_ROOM,
        $1:PT_ER_ADMIT,
        $1:PT_MOTHER_BABY_XREF_1,
        $1:PT_MOTHER_BABY_XREF_2,
        $1:PT_MOTHER_BABY_COMB,
        $1:PT_XRAY_NUMBER,
        $1:PT_REF_DOC_1,
        $1:PT_TRANSACTION_CODE,
        $1:PT_TRANSMISSION_CODE,
        $1:PT_TRANSMITTAL_DATE,
        $1:PT_ORIGINATING_TERM_ID,
        $1:PT_PREV_PATIENT_TYPE,
        $1:PT_PREV_PATIENT_ROOM,
        $1:PT_PREV_PATIENT_BED,
        $1:PT_PRINT_LANGUAGE,
        $1:PT_MSP_PRINT,
        $1:PT_PATIENT_HEIGHT,
        $1:PT_PATIENT_WEIGHT,
        $1:PT_ALLERGIES_CHECKED,
        $1:PT_SOUNDEX
        FROM @ATHENA.<blank>/preprocessing/StCloud/Pulse/patientMaster/patientsMaster.snappy.parquet/patientsMaster.parquet s          
        )
        file_format = andor.master.PARQUETFILETYPE
        on_error = 'continue'


select * 
FROM pulse.masterPatients
limit 100;



copy into stage_patientModel(mrn,firstname, lastname,dob,path,sourceSystems,sourceDataType) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME as path, -- import path
      'HTML' as sourceDataType,
      'Athena Clinical Chart' as sourceSystems
      FROM @ATHENA.<blank>/preprocessing/Bayfront/Athena/drive2/html/ClinicalCharts_PG_81-101-121-141-241-281_A s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/.*/.*[a-z | A-Z]{1,20}[.]png|.*/.*/.*[a-z | A-Z]{1,20}[.]gif';
      
      copy into stage_patientModel(mrn,firstname, lastname,dob,path,sourceSystems,sourceDataType) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME as path, -- import path
      'HTML' as sourceDataType,
      'Athena Clinical Chart' as sourceSystems
      FROM @ATHENA.<blank>/preprocessing/Bayfront/Athena/drive2/html/ClinicalCharts_PG_81-101-121-141-241-281_L s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/.*/.*[a-z | A-Z]{1,20}[.]png|.*/.*/.*[a-z | A-Z]{1,20}[.]gif';
      
      copy into stage_patientModel(mrn,firstname, lastname,dob,path,sourceSystems,sourceDataType) 
FROM (select 
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_',''), -- mrn
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"',''), --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[6],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"',''), --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy'), -- dob
      METADATA$FILENAME as path, -- import path
      'HTML' as sourceDataType,
      'Athena Clinical Chart' as sourceSystems
      FROM @ATHENA.<blank>/preprocessing/Bayfront/Athena/drive2/html/ClinicalCharts_PG_81-101-121-141-241-281_M s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/.*/.*[a-z | A-Z]{1,20}[.]png|.*/.*/.*[a-z | A-Z]{1,20}[.]gif';
      
      
      _____________________




CREATE OR REPLACE TABLE pulse.medications
(
     AccountNumber string,
     MRN int,
     AdmitDate date,
     DischargeDate date,
     FinancialClass string,
     AdmitFinancialClass string,
     DischargeFinancialClass string,
     AdmissionType string,
     AdmissionSource string,
     PatientType string,
     HospitalServiceCode string,
     PhysianNumber string,
     AdmitHospitalServiceCode string
);


copy into stcloud.pulse.medications
FROM 
(
     select 
     $1:AccountNumber,
     $1:MRN ,
     $1:AdmitDate date,
     $1:DischargeDate date,
     $1:FinancialClass ,
     $1:AdmitFinancialClass ,
     $1:DischargeFinancialClass ,
     $1:AdmissionType ,
     $1:AdmissionSource ,
     $1:PatientType ,
     $1:HospitalServiceCode ,
     $1:PhysianNumber ,
     $1:AdmitHospitalServiceCode
     FROM @ATHENA.<blank>/preprocessing/StCloud/Pulse/medications/Medications.parquet/medications.snappy.parquet s
)
File_format= andor.master.PARQUETFILETYPE


select count(*)FROM stcloud.pulse.medications




use andor;
use warehouse andoredw;

-- fact table for visits

CREATE OR REPLACE TABLE public.visits
(
    enterpriseid string default uuid_string(),
    patientid int,
    visitid int,
    providerid int,
    locationid int,
    dateid int,
    deviceid int,
    institutionid int,
    waitingroomid int,
    callid int,
    specialityid int,
    signitureid int,
    paymentid int,
    insuranceid int,
    uploadedDocumentid int,
    downloadedDocuemntid int,
    downloadedTeamsid int,
    invitedMemberid int,
    visitTipsid int,
    updatedMedicalHistoryid int,
    mychartid int,
    educationid int,
    clinicianDashboardid int,
    isVirtualid int,
    admitid int,
    dischargedid int,
    labid int,
    encounterid int,
    socialHistoryId int,
    numberOfPatients int,
    isvalid int,
    validat datetime,
    insertedDate datetime default current_timestamp() 
);




CREATE OR REPLACE VIEW stcloud.public.masterPatientList_vw
as
(
 select distinct
         patientid as Mrn
        ,regexp_substr(upper(regexp_substr(firstname,'[a-z |A-Z]{1,50}')::string),'[A-Z]{1,20}')::string firstname 
        ,upper(trim(regexp_substr(lastname,'[a-z | A-Z]{1,50}'))::string)::string lastname
        ,dob::date as dob 
        --,replace(ssn,'-','')::int as ssn
        ,upper(sex)::string gender
        ,upper(address)::string || ' ' || upper(address2)  as address
        ,regexp_substr(zip,'[0-9]{5}')::string zip
        ,upper(city)::string city
        ,upper(state)::string state
        ,replace
                (
                    replace
                (
                replace
                (
                    replace
                    (
                      regexp_substr
                      (
                          HOMEPHONE,
                          '[0-9]{3}.*[0-9]{3}.*[0-9]{4}'

                      )
                    ,')',''
                    ),'(',''
                ),'-',''
                    
                ),' ',''
                
                ) homephone

        FROM  STCLOUD.athena."patients"
        where regexp_substr(upper(regexp_substr(firstname,'[a-z |A-Z]{1,50}')::string),'[A-Z]{1,20}') not in ('PORTAL','TEST')
        and upper(trim(regexp_substr(lastname,'[a-z | A-Z]{1,50}'))::string) not in ('TEST','DO NOT USE')
        and is_int(mrn)
       
    UNION 
        select distinct
                mrn
               ,regexp_substr(upper(PatientFirstName),'[A-Z]{1,20}') PatientFirstName
               ,regexp_substr(upper(PatientLastName),'[A-Z]{1,20}') PatientLastName
               ,patientdob
               ,upper(gender) gender
               ,upper(PatientAddress) PatientAddress
               ,PatientZip
               ,upper(PatientCity) PatientCity
               ,upper(PatientState) PatientState
               ,PatientPhone
               
        FROM  STCLOUD.ATHENA.ATHENA_CCDA_PATIENTS ccdap
        where regexp_substr(upper(regexp_substr(PatientFirstName,'[a-z |A-Z]{1,50}')::string),'[A-Z]{1,20}') not in ('PORTAL','TEST')
        and upper(trim(regexp_substr(PatientLastName,'[a-z | A-Z]{1,50}'))::string) not in ('TEST','DO NOT USE')
       
    UNION
        select distinct
                mrn
               ,firstname
               ,lastname
               ,dob
               ,null gender
               ,null PatientAddress
               ,null PatientZip
               ,null PatientCity
               ,null PatientState
               ,null PatientPhone
             
        FROM athena.images
        where regexp_substr(upper(regexp_substr(firstname,'[a-z |A-Z]{1,50}')::string),'[A-Z]{1,20}') not in ('PORTAL','TEST')
        and upper(trim(regexp_substr(lastname,'[a-z | A-Z]{1,50}'))::string) not in ('TEST','DO NOT USE')
       
    UNION
        select distinct
                "patientid" mrn
               ,"patient firstname" firstname
               ,"patient lastname" lastname
               ,"patientdob"::date dob
               ,"patientsex" gender
               ,"patient address1" PatientAddress
               , regexp_substr("patient zip",'[0-9]{5}') PatientZip
               ,"patient city" PatientCity
               ,"patient state" PatientState
               ,replace
                (
                    replace
                    (
                        replace
                        (
                            replace
                            (
                              regexp_substr
                              (
                                  case
                                      when "patient homephone" is null then
                                           "patient mobile no"
                                      else "patient homephone"
                                  end,
                                  '[0-9]{3}.*[0-9]{3}.*[0-9]{4}'

                              )
                            ,')',''
                            ),'(',''
                        ),'-',''
                    
                    ),' ',''
                
                )
                PatientPhone
              
        FROM "STCLOUD"."ATHENA"."Sept10DemographicReport"
        where regexp_substr(upper(regexp_substr("patient firstname",'[a-z |A-Z]{1,50}')::string),'[A-Z]{1,20}') not in ('PORTAL','TEST')
        and upper(trim(regexp_substr("patient lastname",'[a-z | A-Z]{1,50}'))::string) not in ('TEST','DO NOT USE')
        
    
        UNION
    
        select distinct
                mrn,
                firstname,
                lastname,
                date(dob,'mm-dd-yyyy') dob,
                null as gender,
                null as PatientAddress,
                null as PatientZip,
                null as PatientCity,
                null as PatientState,
                null as PatientPhone,
               
            FROM athena.pdf
            where mrn is not null
        
)

;



select distinct
        mrn, 
        firstname,
        lastname,
        dob,
        gender,
        address,
        city,
        state,
        zip,
        homephone
FROM stcloud.public.masterPatientList_vw
order by mrn desc ;


