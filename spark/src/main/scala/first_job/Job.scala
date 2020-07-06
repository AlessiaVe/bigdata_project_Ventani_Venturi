import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Application return tuple <Discrict,Description,NumberOfCrimes> ordered by District (asc) then NumberOfCrimes (desc)
 */
object Job extends App {
    val spark = SparkSession
      .builder
      .appName("BDE Spark Ventani Venturi")
      .getOrCreate()

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
}
