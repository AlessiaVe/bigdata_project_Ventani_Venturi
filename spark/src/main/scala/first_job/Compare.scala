import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.storage.StorageLevel

object Compare extends App {
    val spark = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()

    //without join
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
      .orderBy(desc("District"), desc("count"))
      .drop("IUCR")
      .persist(StorageLevel.MEMORY_AND_DISK)

    //with join
    val dfwD = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("/user/sventuri/Crimes-2001-to-present-withoutDescription.csv")
      .select("IUCR","District")
    val dfIUCR = spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("/user/sventuri/Chicago-Police-Department-Illinois-Uniform-Crime-Reporting-IUCR-Codes-nocomma.csv")
      .select("IUCR","Secondary Description")
    dfwD
      .groupBy("IUCR","District")
      .count()
      .orderBy(desc("District"), desc("count"))
      .join(dfIUCR, Seq("IUCR"),"left_outer")
      .drop("IUCR")
      .persist(StorageLevel.MEMORY_AND_DISK)
}