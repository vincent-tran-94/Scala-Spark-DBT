package TestUnitaire

import org.apache.spark.sql.{SparkSession, DataFrame}
import sda.traitement.ServiceVente._

object TestUnitaire {

  def testCalculTTC(data: DataFrame): Unit = {
    val formattedData = data.formatter()
    val result = formattedData.calculTTC()
    result.show()
  }

  def testExtractDateEndContratVille(data: DataFrame): Unit = {
    val result = data.extractDateEndContratVille()
    result.show()
  }

  def testContratStatus(data: DataFrame): Unit = {
    val extractedData = data.extractDateEndContratVille()
    val result = extractedData.contratStatus()
    result.show()
  }

  def runTests(spark: SparkSession): Unit = {
    import spark.implicits._

    // Données pour les tests
    val dataTTC = Seq(
      ("100,00|0,20", "Voiture"),
      ("200,00|0,10", "Ordinateur")
    ).toDF("HTT_TVA", "Produit")

    val dataMeta = Seq(
      ("{\"MetaTransaction\":[{\"Ville\":\"Paris\",\"Date_End_contrat\":\"2023-12-31\"}]}"),
      ("{\"MetaTransaction\":[{\"Ville\":\"Lyon\",\"Date_End_contrat\":\"2024-11-30\"}]}")
    ).toDF("MetaData")

    println("Testing calculTTC...")
    testCalculTTC(dataTTC)

    println("Testing extractDateEndContratVille...")
    testExtractDateEndContratVille(dataMeta)

    println("Testing contratStatus...")
    testContratStatus(dataMeta)
  }


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("TestUnitaire")
      .master("local[*]")
      .getOrCreate()

      runTests(spark)
  }
}
