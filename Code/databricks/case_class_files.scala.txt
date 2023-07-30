%scala
import java.io.File
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import spark.implicits._
import org.apache.spark.sql.Row
 
 
 
val stcloud_athena_ccda = dbutils.fs.ls("dbfs:/mnt/preprocessing/StCloud/Athena/CCDA/").filter(_ isDir).toDS
 
case class CCDA_FILES (Name:String, path:String, size:Long) 
 
val ccda_container = new ListBuffer[CCDA_FILES]()
 
stcloud_athena_ccda.select("path").collect().foreach{row => dbutils.fs.ls( row.toString()
                                                                .replace("[","")
                                                                .replace("]",""))
                                                                .foreach(file => 
                                                                         ccda_container += CCDA_FILES(file.name, file.path,file.size)
                                                                     )}
val athena_stcloud_ccda = ccda_container.toSeq.toDF
print(athena_stcloud_ccda.count)
//ccda_container.toSeq.toDF.registerTempTable("dictionary.stcloud_athena_ccda")
 
