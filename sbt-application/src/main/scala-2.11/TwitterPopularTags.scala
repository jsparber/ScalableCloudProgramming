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
    /* Set logging level if log4j not configured (override by adding log4j.properties to classpath) */
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val sparkSession = SparkSession.builder
      .appName("TwitterPopularTags")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val cb = new ConfigurationBuilder

    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(sc.getConf.get("spark.twitter.consumerKey"))
      .setOAuthConsumerSecret(sc.getConf.get("spark.twitter.consumerSecret"))
      .setOAuthAccessToken(sc.getConf.get("spark.twitter.accessToken"))
      .setOAuthAccessTokenSecret(
        sc.getConf.get("spark.twitter.accessTokenSecret")
      )
    /*FIXME: Not avaible probabily because of old twitter4j version	.setTweetModeExtended(true) */

    val auth = new OAuthAuthorization(cb.build)

    /* Twitter filter */
    val filters = Array[String]()
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, Some(auth), filters)

    val englishTweets = stream.filter(x => x.getLang() == "en" && !x.isRetweet())
    val data = englishTweets.flatMap(status => {
      val text = if (status.isRetweeted) {
        status.getRetweetedStatus.getText
      } else {
        status.getText
      }
      /* TODO: add filter for all special chars */
      Array(
        text.filter(_ >= ' ')
              )
    })

    /* Store every tweet to a text file */
    data.saveAsTextFiles("hdfs://localhost:8020/tweets");

    var countTweet: Long = 0;
    /* Print tweets */
    data.foreachRDD(rdd => {
      var top = rdd.take(10)
      println("\nTweets (%s total):".format(rdd.count()))
      top.foreach { case (count) => println("%s".format(count)) }
      /* Collect at least on RDD with more then 20 tweets */
      countTweet += rdd.count
      if (countTweet > 50) {
        ssc.stop(false)
      }
    })
    ssc.start()
    /* TODO: use a timeout */
    /* Wait for stream to terminate */
    ssc.awaitTermination()

    val docs = sc.textFile("hdfs://localhost:8020/tweets*")

    println("Number of documents to analyze: " + docs.count)

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
    /* Print all records containing only tf (without idf) */
    for (a <- records.collect) {
      println("TF for each document: " + a.weighsVector)
    }

    /* Calculate inverse document frequency */
    val docsSize = docs.count
    val idf = records
      .flatMap(x => x.weighsVector.toList)
      .map({ case (key, _) => (key, 1.0) })
      .reduceByKey(_ + _)
      .map({ case (key, n) => (key, Math.log10(docsSize.toDouble / n)) })

    for (a <- idf.collect) {
      println("IDF for each Token: " + a)
    }

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
