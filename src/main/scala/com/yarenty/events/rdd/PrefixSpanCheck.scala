package com.yarenty.events.rdd

import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.{SparkConf, SparkContext}

object PrefixSpanCheck {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PrefixSpanExample").set("spark.master","local")
    val sc = new SparkContext(conf)

    // $example on$
    val sequences = sc.parallelize(Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6))
    ), 2).cache()
    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(sequences)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
          s" ${freqSequence.freq}")
    }
    
    
    // $example off$

    sc.stop()
  }
}
