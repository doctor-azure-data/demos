import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.xml.XML

object CCDAToParquetConversion {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("CCDA to Parquet Conversion")
      .getOrCreate()

    // Path to the folder containing CCDA XML files
    val ccdaFolderPath = "path/to/ccda_folder"

    // Read all CCDA files from the folder
    val ccdaFiles = spark.read.textFile(ccdaFolderPath)

    // Function to parse CCDA XML and extract relevant data
    def parseCCDA(xmlString: String): Map[String, String] = {
      // Parse XML
      val root = XML.loadString(xmlString)

      // Define namespaces used in CCDA XML
      val ns = Map("ccda" -> "urn:hl7-org:v3")
      // Add other namespaces as required in your CCDA files

      // Extract relevant data elements from XML and create a map
      val ccdaData = Map(
        "patient_name" -> (root \\ "patient" \ "name" \ "given")(0).text,
        "patient_gender" -> (root \\ "patient" \ "administrativeGenderCode")(0).attribute("displayName").getOrElse("").toString,
        "encounter_date" -> (root \\ "effectiveTime")(0).attribute("value").getOrElse("").toString
        // Add more data elements as required based on your CCDA structure
      )

      ccdaData
    }

    // Convert CCDA XML files into a DataFrame
    import spark.implicits._
    val ccdaDataDF: DataFrame = ccdaFiles.map(parseCCDA).toDF()

    // Write the DataFrame to Parquet format
    val outputParquetFolder = "path/to/output_parquet_folder"
    ccdaDataDF.write.parquet(outputParquetFolder)

    // Stop the Spark session
    spark.stop()
  }
}
