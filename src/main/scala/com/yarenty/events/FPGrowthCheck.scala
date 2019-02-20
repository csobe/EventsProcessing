// scalastyle:off

package com.yarenty.events

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


/**
  * Example for mining frequent itemsets using FP-growth.
  * Example usage: ./bin/run-example mllib.FPGrowthExample \
  * --minSupport 0.8 --numPartition 2 ./data/mllib/sample_fpgrowth.txt
  */
object FPGrowthCheck {

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("FPGrowthCheck") {
      head("FPGrowth: an example FP-growth app.")
      opt[Double]("minSupport")
        .text(s"minimal support level, default: ${defaultParams.minSupport}")
        .action((x, c) => c.copy(minSupport = x))
      opt[Double]("minConfidence")
        .text(s"minimal confidence level, default: ${defaultParams.minConfidence}")
        .action((x, c) => c.copy(minConfidence = x))
      opt[Int]("numPartition")
        .text(s"number of partition, default: ${defaultParams.numPartition}")
        .action((x, c) => c.copy(numPartition = x))
      arg[String]("<input>")
        .text("input paths to input data set, whose file format is that each line " +
          "contains a transaction with each item in String and separated by a space")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    //    val conf = new SparkConf().setAppName(s"FPGrowthExample with $params").set("spark.master","local")
    //    val sc = new SparkContext(conf)

    val hConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(params.input))
    println(s"FILE [${params.input}]IS:" + exists)

    val transactions = spark.createDataset(sc.textFile(params.input).map(_.split(",")).cache()).toDF("items")

    println(s"Number of transactions: ${transactions.count()}")

    println(transactions.first().mkString(" + "))

    val fpGrowth = new FPGrowth()
      .setMinSupport(params.minSupport)
      .setNumPartitions(params.numPartition)
      .setMinConfidence(params.minConfidence)

    val model = fpGrowth.fit(transactions)
    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.show()

    println("===============================")
    model.associationRules.show()
    println("=============")

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    val predictions = model.transform(transactions)
    predictions.show


    val milkWithPork = predictions
      .select("*")
      .where(predictions("items")(0).contains("whole milk"))
      .where(predictions("prediction")(0).contains("pork"))
    
    milkWithPork.show(20, false)
    println(milkWithPork.count())

    sc.stop()
  }

  case class Params(
                     input: String = "./data/sample_fpgrowth.txt",
                     minSupport: Double = 0.01,
                     minConfidence: Double = 0.03,
                     numPartition: Int = 1)

}

// scalastyle:on