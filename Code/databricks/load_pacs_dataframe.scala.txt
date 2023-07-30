%scala
 
val files = dbutils.fs.ls("dbfs:/mnt/landing/St.Cloud/PACS/GE/")
 
files.foreach(file =>  spark.read
                            .option("header","true")
                            .option("delimiter","|")
                            .option("inferschema","true")
                            .format("csv")
                            .load("dbfs:/mnt/landing/St.Cloud/PACS/GE/Results_2019-2021_Prod.pacs.txt")
                            .withColumnRenamed("#START","START")
                            .withColumnRenamed("EXAMCODE^EXAMDESCRIPTION","EXAMCODE_EXAMDESCRIPTION")
                            .withColumnRenamed("PATLAST^FIRST^MIDDLE","PATLAST_FIRST_MIDDLE")
                            .withColumnRenamed("DOB(CCYYMMDD)","DOB")
                            .withColumnRenamed("REFPHYSID^LAST^FIRST^MIDDLE","REFPHYSID_LAST_FIRST_MIDDLE")
                            .withColumnRenamed("INTPHYSID^LAST^FIRST^MIDDLE","INTPHYSID_LAST_FIRST_MIDDLE")
                            .withColumnRenamed("IGNPHYSID^LAST^FIRST^MIDDLE","IGNPHYSID_LAST_FIRST_MIDDLE")
                            .withColumnRenamed("PROCEDUREDTTM(CCYYMMDDHHMMSS)","PROCEDUREDTTM")
                            .withColumnRenamed("REPORT TEXT GOES HERE","REPORT")
                            .withColumnRenamed("#END","END")
                            .selectExpr(""" REPLACE(ACCESSION,"|","") as ACCESSION"""
                                        ,"""REPLACE(SPLIT(EXAMCODE_EXAMDESCRIPTION,'\\^')[0],"|","") EXAMCODE"""
                                        ,"""REPLACE(replace(SPLIT(EXAMCODE_EXAMDESCRIPTION,'\\^')[1],"|",""),",","") EXAMCODE_DESCRIPTION"""
                                        ,"""REPLACE(REPORTSTATUS,"|","")REPORTSTATUS"""
                                        ,"""REPLACE(MRN::INT,"|","") MRN"""
                                        ,"""REPLACE(SPLIT(PATLAST_FIRST_MIDDLE,'\\^')[1],"|","") FIRSTNAME"""
                                        ,"""REPLACE(SPLIT(PATLAST_FIRST_MIDDLE,'\\^')[0],"|","") LASTNAME"""
                                        ,"""REPLACE(SPLIT(PATLAST_FIRST_MIDDLE,'\\^')[2],"|","") MIDDLENAME"""
                                        ,"""REPLACE(TO_DATE(STRING(DOB),'yyyyMMdd'),"|","") DOB"""
                                        ,"""REPLACE(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[0],"|","") REF"""
                                        ,"""REPLACE(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[2],"|","") PHY_FIRST_NAME"""
                                        ,"""REPLACE(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[3],"|","") PHY_MID_NAME"""
                                        ,"""REPLACE(SPLIT(REFPHYSID_LAST_FIRST_MIDDLE,'\\^')[1],"|","") PHY_LAST_NAME"""
                                        ,"""REPLACE(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[0],"|","") INIT_PHY_SID"""
                                        ,"""REPLACE(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[2],"|","") INIT_PHY_FIRST_NAME"""
                                        ,"""REPLACE(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[3],"|","") INIT_PHY_MID_NAME"""
                                        ,"""REPLACE(SPLIT(INTPHYSID_LAST_FIRST_MIDDLE,'\\^')[1],"|","") INIT_PHY_LAST_NAME"""
                                        ,"""REPLACE(PROCEDUREDTTM::BIGINT,"|","") PROCEDUREDTTM"""
                                        ,"""REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(report,"|"," "),"~"," "),":",""),"¥",""),"#",""),",","") REPORT"""
                                        )
                          .write
                          .parquet("dbfs:/mnt/preprocessing/StCloud/PACS/preprocessing/StCloud/PACS/GE/parquet/" + file.name.replace("csv","parquet"))
 
             )
