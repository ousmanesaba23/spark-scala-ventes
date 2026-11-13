package fr.master.sparkventes

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

/**
  * Pipeline Spark SQL / DataFrame pour nettoyer, enrichir et exporter des ventes.
  *
  * Arguments :
  *   1. chemin du fichier CSV d'entree ;
  *   2. repertoire CSV de sortie.
  */
object SalesAnalytics {
  private val TimestampPattern = "yyyy-MM-dd HH:mm:ss"

  private val RequiredColumns = Seq(
    "transaction_id",
    "customer_id",
    "transaction_date",
    "product_code",
    "category",
    "city",
    "quantity",
    "unit_price",
    "discount_rate",
    "unit_cost",
    "payment_method"
  )

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(
        "Usage: SalesAnalytics <chemin_ventes.csv> <repertoire_de_sortie>"
      )
      System.exit(2)
    }

    val inputPath = Paths.get(args(0)).toAbsolutePath.normalize()
    val outputPath = Paths.get(args(1)).toAbsolutePath.normalize()

    if (!Files.isRegularFile(inputPath)) {
      throw new IllegalArgumentException(s"Fichier CSV introuvable : $inputPath")
    }
    if (inputPath.startsWith(outputPath)) {
      throw new IllegalArgumentException(
        s"Le repertoire de sortie ne peut pas contenir le fichier source : $outputPath"
      )
    }

    val spark = SparkSession.builder()
      .appName("Nettoyage et analyse des ventes")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")
      runPipeline(spark, inputPath.toString, outputPath.toString)
    } finally {
      spark.stop()
    }
  }

  private def runPipeline(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    // Les colonnes sont d'abord lues comme du texte : chaque conversion est controlee ensuite.
    val raw = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("encoding", "UTF-8")
      .csv(inputPath)

    val missingColumns = RequiredColumns.diff(raw.columns.toSeq)
    require(
      missingColumns.isEmpty,
      s"Colonnes absentes du CSV : ${missingColumns.mkString(", ")}"
    )

    val sourceCount = raw.count()
    val normalized = normalizeAndConvert(raw)
    val clean = applyQualityRules(normalized)
    val cleanCount = clean.count()

    println(s"Lignes lues : $sourceCount")
    println(s"Lignes conservees : $cleanCount")
    println(s"Lignes rejetees : ${sourceCount - cleanCount}")

    val enriched = enrichSales(clean).cache()

    printGlobalSummary(enriched)
    printCategorySummary(enriched)

    enriched.write
      .mode("overwrite")
      .option("header", "true")
      .partitionBy("year", "month")
      .csv(outputPath)

    println(s"Export termine : $outputPath")
    println("Les colonnes year et month sont aussi encodees dans les dossiers de partition.")

    enriched.unpersist()
  }

  /** Nettoie les libelles et convertit les valeurs au type adapte. */
  private def normalizeAndConvert(raw: DataFrame): DataFrame = {
    raw
      .withColumn("transaction_id", trim(col("transaction_id")))
      .withColumn("customer_id", trim(col("customer_id")))
      .withColumn("transaction_date", try_to_timestamp(trim(col("transaction_date")), lit(TimestampPattern)))
      .withColumn("product_code", upper(trim(col("product_code"))))
      .withColumn("category", initcap(lower(trim(col("category")))))
      .withColumn("city", initcap(lower(trim(col("city")))))
      .withColumn("payment_method", upper(trim(col("payment_method"))))
      .withColumn("quantity", expr("try_cast(trim(quantity) AS INT)"))
      .withColumn("unit_price", expr("try_cast(trim(unit_price) AS DECIMAL(18, 2))"))
      .withColumn("discount_rate", expr("try_cast(trim(discount_rate) AS DECIMAL(7, 5))"))
      .withColumn("unit_cost", expr("try_cast(trim(unit_cost) AS DECIMAL(18, 2))"))
  }

  /** Applique les regles de controle qualite de l'enonce. */
  private def applyQualityRules(data: DataFrame): DataFrame = {
    val hasTransactionId = col("transaction_id").isNotNull && length(col("transaction_id")) > 0
    val hasCustomerId = col("customer_id").isNotNull && length(col("customer_id")) > 0

    data.filter(
      hasTransactionId &&
        hasCustomerId &&
        col("transaction_date").isNotNull &&
        col("quantity").isNotNull && col("quantity") > 0 &&
        col("unit_price").isNotNull && col("unit_price") >= 0 &&
        col("unit_cost").isNotNull && col("unit_cost") >= 0 &&
        col("discount_rate").isNotNull &&
        col("discount_rate").between(lit(0).cast(DecimalType(7, 5)), lit(1).cast(DecimalType(7, 5)))
    )
  }

  /** Calcule les montants financiers et les attributs temporels/commerciaux. */
  private def enrichSales(clean: DataFrame): DataFrame = {
    val quantityAsDecimal = col("quantity").cast(DecimalType(12, 0))
    val one = lit(1).cast(DecimalType(7, 5))

    val withAmounts = clean
      .withColumn("gross_amount", round(quantityAsDecimal * col("unit_price"), 2))
      .withColumn(
        "net_amount_ht",
        round(col("gross_amount") * (one - col("discount_rate")), 2)
      )
      .withColumn("total_cost", round(quantityAsDecimal * col("unit_cost"), 2))
      .withColumn("margin", round(col("net_amount_ht") - col("total_cost"), 2))
      .withColumn(
        "margin_rate",
        when(
          col("net_amount_ht") === lit(0).cast(DecimalType(18, 2)),
          lit(null).cast(DecimalType(12, 2))
        ).otherwise(round(col("margin") / col("net_amount_ht") * lit(100), 2))
      )

    withAmounts
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      // Spark utilise 1 pour dimanche et 7 pour samedi.
      .withColumn("day_of_week", dayofweek(col("transaction_date")))
      .withColumn(
        "sale_segment",
        when(col("net_amount_ht") >= 1000, lit("GROS_PANIER"))
          .when(col("net_amount_ht") >= 300, lit("PANIER_MOYEN"))
          .otherwise(lit("PETIT_PANIER"))
      )
      .withColumn(
        "margin_status",
        when(col("margin") < 0, lit("NEGATIVE"))
          .when(col("margin") > 0, lit("POSITIVE"))
          .otherwise(lit("ZERO"))
      )
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("transaction_date"),
        col("product_code"),
        col("category"),
        col("city"),
        col("quantity"),
        col("unit_price"),
        col("discount_rate"),
        col("unit_cost"),
        col("payment_method"),
        col("gross_amount"),
        col("net_amount_ht"),
        col("total_cost"),
        col("margin"),
        col("margin_rate"),
        col("year"),
        col("month"),
        col("day_of_week"),
        col("sale_segment"),
        col("margin_status")
      )
  }

  private def printGlobalSummary(data: DataFrame): Unit = {
    println("\nIndicateurs globaux :")
    data.agg(
      count(lit(1)).as("transactions"),
      round(sum(col("gross_amount")), 2).as("gross_amount_total"),
      round(sum(col("net_amount_ht")), 2).as("net_amount_ht_total"),
      round(sum(col("total_cost")), 2).as("total_cost_total"),
      round(sum(col("margin")), 2).as("margin_total"),
      sum(when(col("margin_status") === "NEGATIVE", 1).otherwise(0)).as("negative_margins")
    ).show(truncate = false)
  }

  private def printCategorySummary(data: DataFrame): Unit = {
    println("\nResultats par categorie :")
    data.groupBy("category")
      .agg(
        count(lit(1)).as("transactions"),
        round(sum(col("net_amount_ht")), 2).as("net_amount_ht"),
        round(sum(col("margin")), 2).as("margin")
      )
      .orderBy(desc("net_amount_ht"))
      .show(truncate = false)
  }
}
