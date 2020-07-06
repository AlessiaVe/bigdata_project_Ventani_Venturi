import org.apache.spark.sql.SparkSession

object DropColumns extends App {
    val spark = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()
    spark.read
      .format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("/user/sventuri/Crimes-2001-to-present.csv")
      .drop("Primary Type","Description")
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("sep", ";")
      .option("header", "true")
      .save("/user/sventuri/Crimes-2001-to-present-withoutDescription.csv")
}