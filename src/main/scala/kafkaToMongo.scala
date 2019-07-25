import java.io.BufferedOutputStream

import com.mongodb.spark.MongoSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset}
import resultcheck.generate

object kafkaToMongo extends App with configuration{


  val dfKafka = createDfKafka()
  dfKafka.show()
  dfKafka.count()
  initifile()
  mongoWrite(dfKafka)
  checkresult(dfKafka)

  def createDfKafka(): DataFrame ={

    import spark.implicits._

    val df: DataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCon)
      .option("subscribe", topic)
      .load()

    val data: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    return data.toDF()

  }


  def mongoWrite (df:DataFrame) {

    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("Overwrite").save()

  }
  def checkresult(dfWrite : DataFrame): Unit =
  {

    var c=""

    val df = MongoSpark.load(spark)
    val dfRead = df.drop("_id")
    val diff = dfRead.except(dfWrite)

    if (diff.count()==0)
    {
      c = generate("passed").toString
    }
    else if(diff.count()!=0) {
      c = generate("failed").toString
    }
    else {
      c = generate("bloqued").toString
    }


    saveResultFile(c)

  }

  def initifile(): Unit =
  {
    val c =generate("failed").toString
    saveResultFile(c)
  }

  def saveResultFile(content: String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", pathHdfs)

    val date = java.time.LocalDate.now.toString
    val filePath = pathResult + resultPrefix + "_" + date + ".json"

    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(filePath), true)

    val writer = new BufferedOutputStream(output)

    try {
      writer.write(content.getBytes("UTF-8"))
    }
    finally {
      writer.close()
    }
  }


}
