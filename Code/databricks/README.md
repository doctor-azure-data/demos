# Featured code

### Databricks Scala code that parses out patients using REGEX and case classes.
---

``` 
val name_pattern = new Regex("(.)+?[A-Z]+(.|-|_)+[A-Z]+(.|-|_)+?[A-Z]+")
val name_pattern_images = new Regex("(zip/)[A-Z & ^0-9]+-[A-Z & ^0-9]+")
val image_pattern = new Regex("[a-z | A-Z0-9]{10}[.][a-z | A-Z]{3}")
val mrn_pattern = new Regex("[0-9]+.pdf")
val dob_pattern = new Regex("[0-9]{2}-[0-9]{2}-[0-9]{4}")
val dob_pattern = new Regex("(_)[0-9]{8}(__)")
val pdf_container = new ListBuffer[PATIENT]()
val ccda_container = new ListBuffer[PATIENT]()
val image_container = new ListBuffer[PATIENT]()
val patient_container = new ListBuffer[PATIENT]()
val encounters = new ListBuffer()()
 
 
case class PATIENT ( FirstName: String
                    ,LastName: String
                    ,Dob: String
                    ,Mrn: String
                    ,FileName:String
                    ,FilePath: String
                    ,FileSize:Long)
 
-- Standardize the delimiter. Some names  were split by and one of [,.-] 
def standardize_del (input: String) : String =
{
  return input.replace(".","-").replaceAll("[-]+","-").replaceAll("_","-")
}
 
-- Standardize the lastnames
def clean_last_name(input: String) : String =
{
  try {return input.replaceFirst("^[-]+","").replace("zip/","").split("-")(0).trim()}
  catch{
    case _: Throwable => println("Problem parsing name " + input.toString())
  }
  return ""
}
 
-- Standardize the firstnames
def clean_first_name(input: String) : String =
{
  try {return input.replaceFirst("^[-]+","").replace("zip/","").split("-")(1).trim()}
  catch{
    case _: Throwable => println("Problem parsing name " + input.toString())
  }
  return ""
}
 
def clean_date(input: String) : String =
{
  return input.replace("_","")
}
 
 
root_folder.select("path").collect()
  .foreach{folder => dbutils.fs.ls(folder.toString()
                                        .replace("[","")
                                        .replace("]",""))
                                          .foreach(subfolder => 
                                           ccda_container += CCDA_FILES(file.name, file.path,file.size)
                                           dbutils.fs.ls(subfolder.path)
                                             .foreach(file => (
                                               if(file.name.endsWith(".xml")) -- If this is an xml file....
                                               {
                                                 ccda_container += (PATIENT(clean_first_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,clean_last_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,(dob_pattern findFirstIn file.name).map(_.toString).getOrElse("01-01-9999")
                                                                         ,file.name
                                                                         ,file.path
                                                                         ,file.size
                                                                         )
                                                                       )
                                                 
                                                  println((PATIENT(clean_first_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,clean_last_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,clean_date((dob_pattern findFirstIn file.name).map(_.toString).getOrElse("01-01-9999"))
                                                                         ,file.name
                                                                         ,file.path
                                                                         ,file.size
                                                                         )
                                                                       ))
                                                 
                                                 
                                                 println(file.name)
 
                                               }
                                               else if(file.name.endsWith(".pdf")) -- if this is a PDF file....
                                               {
 
                                                  pdf_container += (PATIENT(clean_first_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,clean_last_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,(dob_pattern findFirstIn file.name).map(_.toString).getOrElse("01-01-9999")
                                                                         ,(mrn_pattern findFirstIn file.path).map(_.toString).getOrElse("0")
                                                                         ,file.name
                                                                         ,file.path
                                                                         ,file.size
                                                                         )
                                                                       )
 
                                                 
                                                 
                                                  println((PATIENT(clean_first_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,clean_last_name(standardize_del((name_pattern findFirstIn file.name).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,(dob_pattern findFirstIn file.name).map(_.toString).getOrElse("01-01-9999")
                                                                         ,(mrn_pattern findFirstIn file.path).map(_.toString.replace(".pdf","").replace("_"),"").getOrElse("0")
                                                                         ,file.name
                                                                         ,file.path
                                                                         ,file.size
                                                                         )
                                                                       ))
                                                 
 
                                               }
                                               else if(file.name.endsWith(".jpg") || file.name.endsWith(".gif") || file.name.endsWith(".png")) -- If this is a file that is a picture....
                                               {
 
                                                  image_container += ((PATIENT(clean_first_name(standardize_del((name_pattern_images findFirstIn file.path).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,clean_last_name(standardize_del((name_pattern_images findFirstIn file.path).map(_.toString).getOrElse("NONE-NONE")))
                                                                         ,(dob_pattern findFirstIn file.path).map(_.toString).getOrElse("01-01-9999")
                                                                         ,(image_pattern findFirstIn file.name).map(_.toString).getOrElse("ZZZZZZZZZ")
                                                                         ,file.path
                                                                         ,file.size
                                                                         )
                                                                       ))
                                               }
 
 
 
                                             ))
               )}
  
  
 val ccda_df = ccda_container
                 .toSeq
                 .toDF
 
 
try
{
  ccda_df.coalesce(1).write.csv("dbfs:/mnt/preprocessing/StCloud/Athena/ccda_summary.csv")
   pdf_container.toSeqFunctional Requirements
                .toDF
                .coalesce(1)
                .write.option("header","true")
                .csv("dbfs:/mnt/landing/Bayfront/Athena/athena_meta_pdf.csv")
  
     ccda_container.toSeq
                .toDF
                .coalesce(1)
                .write.option("header","true")
                .csv("dbfs:/mnt/landing/Bayfront/Athena/athena_meta_ccda.csv")
  
  
   display(pdf_container)
  
}
catch{
   case _: Throwable => println("Problem writing file: ")
}
 
 
println(ccda_df.summary())
