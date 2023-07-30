%sql
 
select  
  case regexp_extract(ACCESSION,"([0-9]+)") 
    when regexp_extract(ACCESSION,"([0-9]+)") is null
     then 0
    else regexp_extract(ACCESSION,"([0-9]+)") 
    end as ACCESSION,
  regexp_extract(SPLIT(EXAMCODE_EXAMDESCRIPTION,'\\^')[0],"([0-9]+)")::BIGINT EXAMCODE,
  regexp_extract(replace(replace(SPLIT(EXAMCODE_EXAMDESCRIPTION,'\\^')[1],"|",""),",",""),"([A-Z | a-z]+)") EXAMCODE_DESCRIPTION,
  regexp_extract(replace(REPORTSTATUS,"|",""),"([A-Z|a-z]{1})")REPORTSTATUS,
  regexp_extract(mrn,"([0-9]+)")::bigint MRN,
  regexp_extract(replace(SPLIT(PATLAST_FIRST_MIDDLE,'\\^')[1],"|",""),"([A-Z|a-z]+)") FIRSTNAME,
  regexp_extract(replace(SPLIT(PATLAST_FIRST_MIDDLE,'\\^')[0],"|",""),"([A-Z|a-z]+)") LASTNAME,
  regexp_extract(replace(SPLIT(PATLAST_FIRST_MIDDLE,'\\^')[2],"|",""),"([A-Z|a-z]+)") MIDDLENAME,
  TO_DATE(regexp_extract(DOB,"([0-9]{8})"),'yyyyMMdd') DOB,
  regexp_extract(replace(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[0]::INT,"|",""),"([0-9]+)") REF,
  regexp_extract(replace(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[2],"|",""),"([A-Z|a-z]+( )?)") PHY_FIRST_NAME,
  regexp_extract(replace(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[3],"|",""),"([A-Z|a-z]+( )?)") PHY_MID_NAME,
  regexp_extract(replace(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[1],"|",""),"([A-Z|a-z]+( )?)") PHY_LAST_NAME,
  regexp_extract(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[0],"([0-9]+)") INIT_PHY_SID,
  regexp_extract(replace(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[2],"|",""),"([A-Z | a-z])") INIT_PHY_FIRST_NAME,
  regexp_extract(replace(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[3],"|",""),"([A-Z | a-z])") INIT_PHY_MID_NAME,
  regexp_extract(replace(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[1],"|",""),"([A-Z | a-z])") INIT_PHY_LAST_NAME,
  regexp_extract(PROCEDUREDTTM,"([0-9]+)") PROCEDUREDTTM,
  replace(replace(replace(replace(replace(replace(REPORT,":",""),'+-----------------------+',' '),'~',' '),'_______________________________________________________________________',' '),'#END',''),',','.') REPORT
  from 2021_05_28_03_38_23_073_pacs_txt
