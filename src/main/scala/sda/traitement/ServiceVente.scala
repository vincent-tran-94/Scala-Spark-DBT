package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }
    def calculTTC() : DataFrame = {

      // Conversion des colonnes HTT et TVA en Double (en s'assurant que les virgules sont remplacées par des points)
      var dfwithTTC = dataFrame
        .withColumn("HTT", regexp_replace(col("HTT"), ",", "."))  // Remplacer la virgule par un point
        .withColumn("TVA", regexp_replace(col("TVA"), ",", "."))  // Remplacer la virgule par un point

      dfwithTTC = dfwithTTC.withColumn("TTC", round(col("HTT") + (col("TVA") * col("HTT")), 2))
      dfwithTTC = dfwithTTC.drop("TVA", "HTT")
      dfwithTTC
    }
    def extractDateEndContratVille(): DataFrame = {
      // Définition du schéma pour la colonne JSON
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, true)
        .add("Date_End_contrat", StringType, true)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      val extractedData = dataFrame
        .withColumn("MetaData_Parsed", from_json(col("MetaData"), schema)) // Convertir JSON en structure
        .withColumn("Ville", expr("MetaData_Parsed.MetaTransaction[0].Ville")) // Extraire Ville
        .withColumn("Date_End_contrat_raw", expr("MetaData_Parsed.MetaTransaction[0].Date_End_contrat")) // Extraire Date_End_contrat brute
        .withColumn("Date_End_contrat", regexp_extract(col("Date_End_contrat_raw"), "(\\d{4}-\\d{2}-\\d{2})", 1)) // Formatter YYYY-MM-DD
        .drop("MetaData","MetaData_Parsed","Date_End_contrat_raw") // Supprimer les colonnes inutiles

      extractedData
    }

    def contratStatus(): DataFrame = {
      var contratData = dataFrame.withColumn(
        "Contrat_Status",
        when(to_date(col("Date_End_contrat"), "yyyy-MM-dd") < current_date(), lit("Expired"))
          .otherwise(lit("Actif"))
      )
      contratData
    }
  }
}
