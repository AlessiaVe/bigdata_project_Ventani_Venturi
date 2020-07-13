import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, desc}

/**
 * Application return tuple <Discrict,Description,NumberOfCrimes> ordered by District (asc) then NumberOfCrimes (desc)
 * using two separated tables joined by IUCR field.
 */
object CountCrimesWithJoin extends App {

    val spark = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()
    val sc = spark.sparkContext

    var inputFile = ""
    var iucrFile = ""
    var outputFile = ""
    if(args.toList.size != 3){
        inputFile = "/user/sventuri/Crimes-2001-to-present-withoutDescription.csv"
        iucrFile = "/user/sventuri/Chicago-Police-Department-Illinois-Uniform-Crime-Reporting-IUCR-Codes-nocomma.csv"
        outputFile = "/user/sventuri/Count-Crimes-2001-to-Present-WithJoin"
    } else {
        inputFile = args.toList.head
        iucrFile = args.toList.tail.head
        outputFile = args.toList.tail.tail.head
    }
    val outputPath = new Path(outputFile)

    val fileSystem = FileSystem.get(new Configuration())

    if (fileSystem.exists(outputPath)) {
        fileSystem.delete(outputPath, true)
    }

    val dfwD = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(inputFile)
      .select("IUCR","District")
    val dfIUCR = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(iucrFile)
      .select("IUCR","Secondary Description")

    //versione broadcast

    /*val bdfIUCR = spark.sparkContext.broadcast(dfIUCR)
    dfwD
      .groupBy("IUCR","District")
      .count()
      .orderBy(asc("District"), desc("count"))
      .join(bdfIUCR.value, dfwD("IUCR") <= dfIUCR("IUCR"))
      .drop("IUCR")
      //.coalesce(1)
      .write.format("csv")
      .option("sep", ";")
      .save(outputFile)*/

    //versione no broadcast
    dfwD
      .groupBy("IUCR","District")
      .count()
      .orderBy(asc("District"), desc("count"))
      .join(dfIUCR, dfwD("IUCR") === dfIUCR("IUCR"), "left_outer")
      .drop("IUCR")
      //.coalesce(1)
      .write.format("csv")
      .option("sep", ";")
      .save(outputFile)
}