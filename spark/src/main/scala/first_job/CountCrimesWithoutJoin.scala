import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc,desc}

/**
 * Application return tuple <Discrict,Description,NumberOfCrimes> ordered by District (asc) then NumberOfCrimes (desc)
 */
object CountCrimesWithoutJoin extends App {

    val spark = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()

    var inputFile = ""
    var outputFile = ""
    if(args.toList.size != 2){
        inputFile = "/user/sventuri/Crimes-2001-to-present-nocomma.csv"
        outputFile = "/user/sventuri/Count-Crimes-2001-to-Present"
    } else {
        inputFile = args.toList.head
        outputFile = args.toList.tail.head
    }
    val outputPath = new Path(outputFile)
    val fileSystem = FileSystem.get(new Configuration())
    if (fileSystem.exists(outputPath)) {
        fileSystem.delete(outputPath, true)
    }

    val df = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("/user/sventuri/Crimes-2001-to-present-nocomma.csv")
      .select("IUCR","District", "Description")
    df
      .groupBy("IUCR","District","Description")
      .count()
      .orderBy(asc("District"), desc("count"))
      .drop("IUCR")
      //.coalesce(1)
      .write.format("csv")
      .option("sep", ";")
      .save(outputFile)
}
