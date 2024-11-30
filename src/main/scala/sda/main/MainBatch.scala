package sda.main
import org.apache.spark.sql.SparkSession
import sda.args._
import sda.parser.ConfigurationParser
import sda.traitement.ServiceVente._

object MainBatch {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("SDA")
      .config("spark.master", "local")
      .getOrCreate()

    Args.parseArguments(args)

    val reader = Args.readertype match {
      case "csv" => ConfigurationParser.getCsvReaderConfigurationFromJson(Args.readerConfigurationFile)
      case "json" => ConfigurationParser.getJsonReaderConfigurationFromJson(Args.readerConfigurationFile)
      case "xml"  => ConfigurationParser.getXmlReaderConfigurationFromJson(Args.readerConfigurationFile)
      case _ => throw new Exception("Invalid reader type. Supported reader format: csv, json, xml")
    }

    // Lire le DataFrame
    val df = reader.read().formatter()

    println("*********************** Résultat Question1 *****************************")
    df.show(20)
    df.printSchema()
    println("*********************** Résultat Question2 *****************************")
    // Appliquer la fonction calculTTC
    df.calculTTC().show(20)
    println("*********************** Résultat Question3 *****************************")
    // Exemple pour d'autr)es transformations
    df.calculTTC().extractDateEndContratVille().show()
    println("*********************** Résultat Question4 *****************************")
    df.calculTTC().extractDateEndContratVille().contratStatus().show(20)
  }
}
