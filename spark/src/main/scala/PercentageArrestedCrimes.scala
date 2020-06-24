import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, round, asc, avg, when}

object PercentageArrestedCrimes extends App {

  override def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("{absolute path to input file} {absolute path to output file}")
      return
    }

    val spark: SparkSession = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()
    import spark.implicits._
    // check file
    val inputFile = args(0)
    val outputFile = args(1)
    val inputPath = new Path(inputFile)
    val outputPath = new Path(outputFile)


    val fileSystem = FileSystem.get(new Configuration())
    if (!fileSystem.exists(inputPath)) {
      println("Invalid input path")
      return
    }

    if (fileSystem.exists(outputPath)) {
      println("Output path exists -> delete file")
      fileSystem.delete(outputPath, true)
    }

    spark.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(inputFile)                                                    // read the file
      .select("Year", "Description", "Arrest", "IUCR")                    // select the column
      .groupBy("Year", "IUCR", "Description")                             // group by year, IUCR and Description
      .agg(round(avg(when($"Arrest" === "true", 1).otherwise(0)*100)).as("Percentage crimes with arrest"))  // sum on Arrest and calculate percentage
      .orderBy(asc("Year"))                                               // order by year
      .select("Year", "Description", "Percentage crimes with arrest")     // output
      .write.format("csv")                                                // save output on file
      .option("sep", ";")
      .save(outputFile)
  }
}