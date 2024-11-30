package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class JsonReader(path: String,
                      multiline: Option[Boolean] = None // Ajout de l'option multiline
                     ) extends Reader {

  val format = "json"

  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format)
      .option("multiline", multiline.getOrElse(true)) // Utilisation de l'option multiline
      .load(path)
  }
}
