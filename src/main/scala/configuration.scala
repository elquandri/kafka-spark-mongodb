import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

trait configuration {


  val config:Config=ConfigFactory.load("KafkaMongDB.conf")
  val kafkaProd =config.getString("kafkaProd")
  val kafkaCon =config.getString("kafkaCon")
  val topic=config.getString("topic")
  val appName=config.getString("appName")
  val master =config.getString("master")
  val server =config.getString("server")
  val pathResult =config.getString("pathResult")
  val resultPrefix = config.getString("resultPrefix")
  val pathHdfs = config.getString("pathHdfs")
  val inputUrl =config.getString("inputUrl")
  val outputUrl =config.getString("outputUrl")
  val dbConnection =config.getString("dbConnection")
  val odbConnection =config.getString("odbConnection")
  val dbName =config.getString("dbName")
  val collectionConnection =config.getString("collectionConnection")
  val ocollectionConnection =config.getString("ocollectionConnection")
  val collectionName =config.getString("collectionName")
  val mongo_host =config.getString("mongo_host")


  // sparkConsumer configuration
  val spark = SparkSession.builder.appName(appName).master(master)
    .config(inputUrl, mongo_host)
    .config(dbConnection, dbName)
    .config(collectionConnection, collectionName)
    .config(outputUrl, mongo_host)
    .config(odbConnection, dbName)
    .config(ocollectionConnection, collectionName).getOrCreate()

  // configuration du topic
  val topics = List(topic)


}
