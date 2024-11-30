name := "projet_sda_2024"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "com.beust" % "jcommander" % "1.48"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.15.0" // Il faut insérer cette dépendance pour pouvoir lire le fichier XML
