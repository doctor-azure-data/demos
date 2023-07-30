val root_folder = dbutils.fs.ls("dbfs:/mnt/preprocessing/StCloud/Pulse/").filter(_ isDir).toDS
 
val visit_df = spark.read
                    .option("delimiter","|")
                    .option("header","true")
                    .option("inferschema", "true") 
                    .format("csv")
                    .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/")
 
 
val pat_info_df = spark.read
                       .option("delimiter","|")
                       .option("header","true")
                       .option("inferschema", "true") 
                       .format("csv")
                       .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/patientInformation.txt")
 
val mpi_df = spark.read
                  .option("header","true")
                  .option("delimiter","|")
                  .option("inferschema", "true")
                  .format("csv")
                  .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/mpi.txt")
              
val diagnosis_df = spark.read
                        .option("header","true")
                        .option("delimiter","|")
                        .option("inferschema", "true")   
                        .format("csv")
                        .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/diagnosis.txt")
  
val physicians_df = spark.read
                         .option("delimiter","|")
                         .option("header","true")
                         .option("inferschema", "true") 
                         .format("csv")
                         .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/physicians.txt")
 
 
val cpt_df = spark.read
                  .option("delimiter","|")
                  .option("header","true")
                  .option("inferschema", "true") 
                  .format("csv")
                  .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/cpt.txt")
 
  
val procedures_df = spark.read
                         .option("header","true")
                         .option("delimiter","|")
                         .option("inferschema", "true")  
                         .format("csv")
                         .load("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/procedures.txt")
 
val patients_df = spark.read
                       .option("header","true")
                       .option("delimiter",",")
                       .format("csv")
                       .load("dbfs:/mnt/preprocessing/StCloud/Pulse/PullPatientMasterfromPulse-Adam.zip/")
//display(master_df)        
 
// visit_df.coalesce(1)
//                     .write
//                     .option("header","true")
//                     .csv("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/visit.csv")
// pat_info_df.coalesce(1)
//                     .write
//                     .option("header","true")
//                     .csv("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/pat_info_df.csv")
// mpi_df.coalesce(1)
//                     .write
//                     .option("header","true")
//                     .csv("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/mpi_df.csv")
// diagnosis_df.coalesce(1)
//                     .write
//                     .option("header","true")
//                     .csv("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/diagnosis_df.csv")
// cpt_df.coalesce(1)
//                     .write
//                     .option("header","true")
//                     .csv("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/cpt_df.csv")
// procedures_df.coalesce(1)
//                     .write
//                     .option("header","true")
//                     .csv("dbfs:/mnt/preprocessing/StCloud/Pulse/CHSEMPIExport9-10-2020.zip/procedures_df.csv")
 
 
 
patients_df.createTempView("mpi_df")
 
 
 
//display(patients_df)
 
 
// visit_df.withColumnRenamed("Account Number","AccountNumber")
//         .withColumnRenamed("Diag Type","DiagType")
//         .withColumnRenamed("Diag Code","DiagCode")
//         .withColumnRenamed("Date of Onset","DateOfOnset")
//         .withColumnRenamed("Present on Admit","PresentOnAdmit")
//         .withColumnRenamed("ICD Type","ICDTYPE")
//         .withColumnRenamed("Sequence                 ","Sequence")
//         .createOrReplaceTempView("visits")
