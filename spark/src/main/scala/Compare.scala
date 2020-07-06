import org.apache.spark.sql.SparkSession

object Compare extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("BDE Spark Ventani Venturi").getOrCreate()
    val sc = spark.sparkContext

    val dfCountCrimeswithoutJoin = sc.textFile("hdfs:Crimes-2001-to-present.csv").map(CrimesData.extract)
      .map(x => (x.description, x.district))
      .groupBy(e => (e._1, e._2))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .sortBy(r => (r._1._1._2, r._2, false))

    val dfwD = sc.textFile("hdfs:Crimes-2001-to-present-withoutDescription.csv").map(CrimesWithoutDescriptionData.extract)
    val bDfIUCR = sc.broadcast(sc.textFile("hdfs:Chicago-Police-Department-Illinois-Uniform-Crime-Reporting-IUCR-Codes.csv").map(IucrData.extract))
    val dfWithJoin = dfwD
      .groupBy(x => (x.iucr, x.district))
      .map(e => (e, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1._1._2, bDfIUCR.value.map(x => x.secondaryDescription), x._2))
      .sortBy(r => (r._1, r._3, false))
  }
}
