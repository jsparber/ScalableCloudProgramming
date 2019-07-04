/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TwitterPopularTags {
  def main(args: Array[String]) {
    /* Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.OFF)
    }
     */

    /* Overwrite Log level */
    Logger.getRootLogger.setLevel(Level.OFF)

    val sparkSession = SparkSession.builder
      .appName("TwitterPopularTags")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val docs = sc.textFile("hdfs:///tweets*")

    /* Calculate word frequency */
    val records = docs.map(doc => {
      val wordArray = doc.split(" ")
          .filter(s => !s.contains("http"))
          .flatMap(_.replaceAll("[^A-Za-z|^#]", " ").split(" "))
          .filter(_.length > 1)
      //  doc.replaceAll("[^A-Za-z]", " ").split(" ").filter(!_.isEmpty)

      /* reduceByKey for array */
      val map = wordArray
        .map(word => (word.toLowerCase, 1))
        .groupBy(_._1)
        .map(l => (l._1, l._2.map(_._2).reduce(_ + _)))
        .map({ case (key, n) => (key, n.toDouble / wordArray.length) })
        new TweetTF(doc, map)
    })

    /* Calculate inverse document frequency */
    val docsSize = docs.count
    val idf = records
      .flatMap(x => x.weighsVector.toList)
      .map({ case (key, _) => (key, 1.0) })
      .reduceByKey(_ + _)
      .map({ case (key, n) => (key, Math.log10(docsSize.toDouble / n)) })

    /* FIXME: don't use collect, but probably there isn't a better solution
     *  because collect moves all data back to the driver application */
    val table = idf.collect.toMap
    /* create vector for each document as a Map */
    val dbscan_records = records.map(m => new Record(m, table)) //calculate tfidf, for each value of map it is transformed into the new value
    for (a <- dbscan_records.collect) {
      println("Vector for each document: " + a)
    }

    SequentialDBSCAN.exec(dbscan_records)
    sparkSession.stop()
  }
}
// scalastyle:on println
