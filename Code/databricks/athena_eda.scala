This is a consolidated script. I had to clean up the PHI.

%sql
show tables;

desc oh.ccda

%scala
val data = spark.sql("select * from oh.ccda");

%sql
 

select alternateMrn, count(*)
from oh.ccda
where alternateMrn is not null
group by alternateMrn

%scala
data.describe("mrn","alternateMrn","PatientName","PatientLastName","patientDOB","patientPhone").show()


select count(distinct mrn) from athena.ccda;


54,256 disinct MRN's, when we compare this to the known total patient count of 81,313 (From patients.csv) - (minus) the known missing MRN's, this looks normal.


SELECT mrn,regexp_extract(sourceFIle, '([0-9][0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9][0-9][0-9][0-9])', 1) imputedMRN
from athena.ccda 
where mrn is not null;

%sql
create view oh.patientDemogrphics_vw
AS 
(
  SELECT mrn,
       alternateMrn,
       get_json_object(PatientStreetAddress, '$.city.value') city, 
       get_json_object(PatientStreetAddress, '$.country') country, 
       get_json_object(PatientStreetAddress, '$.postalCode.value') zip,
       get_json_object(PatientStreetAddress, '$.state.value') state,
       get_json_object(PatientStreetAddress, '$.streetAddressLine[0].value') addr,
       PatientPhone
FROM oh.ccda
);

