import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


/**
 * Application return a csv file without Primary Type and Description columns
 *
 */
object DropColumns extends App {
  val spark = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()
  val outputFile = "/user/sventuri/Crimes-2001-to-present-withoutDescription.csv"
  val outputPath = new Path(outputFile)
  val fileSystem = FileSystem.get(new Configuration())
  if (fileSystem.exists(outputPath)) {
    fileSystem.delete(outputPath, true)
  }
    spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("/user/sventuri/Crimes-2001-to-present-nocomma.csv")
      .drop("Primary Type","Description")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("sep", ";")
      .option("header", "true")
      .save(outputFile)
}