package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class XmlReader(path: String,
                     rootElement: Option[String] = None,
                     recordElement: Option[String] = None
                    ) extends Reader {
  val format = "xml"

  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format)
      .option("rootTag", rootElement.getOrElse("clients"))
      .option("rowTag", recordElement.getOrElse("client"))
      .load(path)
  }
}
