package com.yarenty.events.rdd


import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.reflect.runtime.universe._

/**
  * Abstract class for parameter case classes.
  * This overrides the [[toString]] method to print all case class fields by name and value.
  * @tparam T  Concrete parameter class.
  */
abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  /**
    * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
    * {
    *   [field name]:\t[field value]\n
    *   [field name]:\t[field value]\n
    *   ...
    * }
    */
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}

/**
  * Example for mining frequent itemsets using FP-growth.
  * Example usage: ./bin/run-example mllib.FPGrowthExample \
  *   --minSupport 0.8 --numPartition 2 ./data/mllib/sample_fpgrowth.txt
  */
object FPGrowthCheck {

  case class Params(
                     input: String = "./data/sample_fpgrowth.txt",
                     minSupport: Double = 0.01,
                     numPartition: Int = 1) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("FPGrowthExample") {
      head("FPGrowth: an example FP-growth app.")
      opt[Double]("minSupport")
        .text(s"minimal support level, default: ${defaultParams.minSupport}")
        .action((x, c) => c.copy(minSupport = x))
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
    val conf = new SparkConf().setAppName(s"FPGrowthExample with $params").set("spark.master","local")
    val sc = new SparkContext(conf)

    val hConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(params.input))
    println(s"FILE [${params.input}]IS:" + exists)
    
    val transactions = sc.textFile(params.input).map(_.split(",")).cache()

    println(s"Number of transactions: ${transactions.count()}")

    println(transactions.first().mkString(" + "))
    
    val fpGrowth = new FPGrowth()
      .setMinSupport(params.minSupport)
      .setNumPartitions(params.numPartition)
     

    val model = fpGrowth
      .run(transactions)
    
    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.collect().foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")}, ${itemset.freq}")
    }

    println("===============================")
    val minConfidence = 0.03
    val rules = model.generateAssociationRules(minConfidence).collect()
      rules.foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
    }
   
    println("=============")
    println(rules(5).antecedent.mkString(" => "))
    println(rules(5).consequent.mkString(" => "))
    println(rules(5).confidence)
    println(rules(5).lift)
    println(rules(5).toString())
    
    /* THIS WILL NOT WORK !!! item support is calculated based on the count of all apperances! 
   val model2 = fpGrowth.run(transactions)
    val freqItemsets = model2.freqItemsets ++ model.freqItemsets
    val itemSupport = model2.itemSupport ++ model.itemSupport
    val newModel = new FPGrowthModel(freqItemsets, itemSupport)
    
    val newRules = newModel.generateAssociationRules(minConfidence).collect()


    println("UPDATED =============")

    newRules.foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
    }


    println(newRules(5).antecedent.mkString(" => "))
    println(newRules(5).consequent.mkString(" => "))
    println(newRules(5).confidence)
    println(newRules(5).lift)
    println(newRules(5).toString())
    
    */
    
    sc.stop()
  }
}