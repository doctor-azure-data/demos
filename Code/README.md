# Featured Code

```COPY INTO image_model(firstname, lastname,dob,mrn, sourceSystems, path) 
FROM (SELECT 
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[5],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[0],'"','') firstname, --fname
      replace(split(regexp_substr(split(METADATA$FILENAME,'/')[5],'[A-Z]{1,20}-[A-Z]{1,20}'),'-')[1],'"','') lastname, --lanme
      date(replace(regexp_substr(METADATA$FILENAME,'_[0-9]{2}-[0-9]{2}-[0-9]{4}_'),'_',''),'mm-dd-yyyy') dob, -- dob
      replace(regexp_substr(METADATA$FILENAME,'_[0-9]{4,20}_'),'_','')mrn, -- mrn
      'StCloud Athena Clinical Chart' sourcesystems,
      METADATA$FILENAME path -- import path
      FROM @ATHENA.<blank>/preprocessing/StCloud/Athena/HTML/ClinicalCharts_PG_26-261_A s)
      file_format = <blank>.APPOINTMENTS.HTML
      pattern = '.*/[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}';```
