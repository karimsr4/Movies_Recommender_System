package similarity

import org.rogach.scallop._
import org.json4s.jackson.{Serialization, compactJson}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)


  // This method is used to load data
  // The refactoring of loading data is useful to get over caching issues in time performance measurements
  def load_data(): (RDD[Rating], RDD[Rating]) = {

    val trainFile = spark.sparkContext.textFile(conf.train())
    val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    })

    assert(train.count == 80000, "Invalid training data")

    val testFile = spark.sparkContext.textFile(conf.test())
    val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    })
    assert(test.count == 20000, "Invalid test data")

    (train, test)
  }


  // ************************************************************************

  // ************************** Helper functions ****************************
  // compute the scale
  def scale(r: Double, av: Double): Double = {
    if (r > av) {
      5 - av
    } else if (r < av) {
      av - 1
    } else {
      1
    }
  }

  def normalize(x: Double, y: Double): Double = {
    (x - y) / scale(x, y)
  }

  //*************************************************************************


  // This function computes the preprocessing scores needed for similarity computations
  // returns an RDD of tuples (user, item, score)
  def preprocess(data: RDD[Rating]): RDD[(Int, Int, Double)] = {

    // average rating per user
    val ratings_per_user = data.groupBy(_.user)
      .mapValues(l => l.map(_.rating).sum / l.size)


    val deviaFromUserAverage = data.map(r => (r.user, r))
      .join(ratings_per_user)
      .map(_._2)
      .map { case (x, y) => (x, normalize(x.rating, y)) }


    val denom = deviaFromUserAverage.groupBy { case (r: Rating, dev) => r.user }
      .mapValues(l => math.sqrt(l.map { case (r, dev) => dev * dev }.sum))


    val preprocessed = deviaFromUserAverage.map { case (r, dev) => (r.user, (r, dev)) }
      .join(denom)
      .mapValues { case ((r, dev), denominator) => if (denominator == 0) (r, 0.0) else (r, dev / denominator) }
      .map(_._2)

    preprocessed.map { case (r, preprocess) => (r.user, r.item, preprocess) }

  }


  // computes all similarities, return a Map of u1 -> Map(u2, similarity_1_2) where u1<=u2
  // returns also the time needed to compute the similarities
  def compute_cosine_similarity(): (collection.Map[Int, Map[Int, Double]], Long) = {

    val (train, test) = load_data()

    // preprocessing step

    val preprocessed = preprocess(train).map { case (u, i, score) => (i, (u, score)) }

    val similarities = preprocessed.join(preprocessed)
      .filter { case (i, ((u1, s1), (u2, s2))) => u1 <= u2 }
      .map { case (i, ((u1, s1), (u2, s2))) => ((u1, u2), s1 * s2) }


    val start = System.currentTimeMillis()

    similarities.reduceByKey(_ + _).map(_._2).reduce(_ + _) // forcing the materialisation of the rdd to compute the time

    val end = System.currentTimeMillis()

    val sim_computation_time = (end - start) * 1000

    val result = similarities.reduceByKey(_ + _)
      .filter(_._2 != 0.0)
      .map { case ((u1, u2), s12) => (u1, (u2, s12)) }
      .groupByKey()
      .mapValues(l => l.toMap)
      .collectAsMap()

    (result, sim_computation_time)

  }


  // This function computes the Jaccard similarity between users
  def compute_jaccard_similarity(train: RDD[Rating]): collection.Map[Int, Map[Int, Double]] = {

    val m = train.map(p => (p.item, (p.user, p.rating)))

    // Map contains the number of ratings per each user
    val count_rating_per_user = train.map(p => (p.user, (p.item, p.rating)))
      .groupByKey()
      .mapValues(l => l.size)
      .collectAsMap()

    val intersections = m.join(m)
      .filter { case (i, ((u1, r1), (u2, r2))) => u1 < u2 } // only compute on different users and with no repetitive computation
      .map { case (i, ((u1, r1), (u2, r2))) => ((u1, u2), 1) }
      .reduceByKey(_ + _)

    // a Map of u -> Map ( v -> similarity)
    val jaccardMap = intersections.map { case ((u1, u2), inter) => ((u1, u2), (inter, count_rating_per_user.getOrElse(u1, 0), count_rating_per_user.getOrElse(u2, 0))) }
      .mapValues { case (inter, a, b) => inter.toDouble / (a + b - inter).toDouble }
      .map { case ((u1, u2), index) => (u1, (u2, index)) }
      .groupByKey()
      .mapValues(l => l.toMap)
      .collectAsMap()


    jaccardMap

  }


  // This function is used to compute the number of multiplications required for each s_uv in question 2.2.4
  // returns the required statistics i.e (min ,max, average, stdev)
  // to be completed
  def compute_multiplications_needed(train: RDD[Rating]): (Int, Int, Double, Double) = {

    // RDD with the set of items rated for each user
    val items_per_user = train.groupBy(_.user).mapValues(l => l.map(_.item).toSet).collect()
    val result = items_per_user.flatMap { case (u1, set1) => items_per_user.map { case (u2, set2) => (u1, u2, set1.intersect(set2).size) } }.filter(p => p._1 <= p._2).map(_._3).toList
    val mean = result.reduce(_ + _) / result.length.toDouble
    val std = math.sqrt(result.map(r => (r - mean) * (r - mean)).reduce(_ + _) / result.length.toDouble)

    (result.min, result.max, mean, std)


  }


  // This function computes the predictions using Adjusted cosine similarity
  // It returns the whole prediction time, the similarity computation time and the MAE
  def predict_byCosineSimilarity(): (Long, Long, Double) = {

    val (train, test) = load_data()
    // average rating per user
    val ratings_per_user = train.groupBy(_.user)
      .mapValues(l => l.map(_.rating).sum / l.size)

    // deviation from user average for each rating in the train set
    val deviaFromUserAverage = train.map(r => (r.user, r))
      .join(ratings_per_user)
      .map(_._2)
      .map { case (x, y) => (x.item, (x.user, normalize(x.rating, y))) }


    // similarity between users and time to compute the similarity
    val (sim, sim_time) = compute_cosine_similarity()


    val predictions = test.map(p => (p.item, (p.user, p.rating)))
      .leftOuterJoin(deviaFromUserAverage)
      .map { case (i, ((u, r), Some((v, dev)))) => ((u, i, r), (sim.get(scala.math.min(u, v)).get.getOrElse(scala.math.max(u, v), 0.0) * dev, scala.math.abs(sim.get(scala.math.min(u, v)).get.getOrElse(scala.math.max(u, v), 0.0))))
      case (i, ((u, r), None)) => ((u, i, r), (0.0, 1.0))
      }.reduceByKey((p, q) => (p._1 + q._1, p._2 + q._2))
      .mapValues { case (num, denom) => if (denom == 0) 0 else num / denom }
      .map { case ((u, i, r), s) => (u, (i, r, s)) }
      .join(ratings_per_user)
      .map { case (u, ((i, r, s), av)) => (u, i, r, av + s * scale(av + s, av)) }

    val start = System.currentTimeMillis()
    predictions.map(p => scala.math.abs(p._3 - p._4)).reduce(_ + _) // materialize the rdd to get the computation time
    val end = System.currentTimeMillis()

    val mae = predictions.map(p => scala.math.abs(p._3 - p._4)).sum / test.count.toDouble

    val elapsed = (end - start) * 1000 // from milliseconds to microseconds


    (elapsed + sim_time, sim_time, mae)


  }

  // This function computes the predictions using Adjusted cosine similarity
  // It returns the whole prediction time, the similarity computation time and the MAE
  def predict_byJaccardSimilarity(): Double = {

    val (train, test) = load_data()
    // average rating per user
    val ratings_per_user = train.groupBy(_.user)
      .mapValues(l => l.map(_.rating).sum / l.size)

    // deviation from user average for each rating in the train set
    val deviaFromUserAverage = train.map(r => (r.user, r))
      .join(ratings_per_user)
      .map(_._2)
      .map { case (x, y) => (x.user, x.item, normalize(x.rating, y)) }.map { case (u, i, dev) => (i, (u, dev)) }


    // similarity between users in the form of ( (u1,u2), similarity)
    val sim = compute_jaccard_similarity(train)


    val predictions = test.map(p => (p.item, (p.user, p.rating)))
      .leftOuterJoin(deviaFromUserAverage)
      .map { case (i, ((u, r), Some((v, dev)))) => ((u, i, r), (sim.get(scala.math.min(u, v)).get.getOrElse(scala.math.max(u, v), 0.0) * dev, scala.math.abs(sim.get(scala.math.min(u, v)).get.getOrElse(scala.math.max(u, v), 0.0))))
      case (i, ((u, r), None)) => ((u, i, r), (0.0, 1.0))
      }.reduceByKey((p, q) => (p._1 + q._1, p._2 + q._2))
      .mapValues { case (num, denom) => if (denom == 0) 0 else num / denom }
      .map { case ((u, i, r), s) => (u, (i, r, s)) }
      .join(ratings_per_user)
      .map { case (u, ((i, r, s), av)) => (u, i, r, av + s * scale(av + s, av)) }


    val mae = predictions.map(p => scala.math.abs(p._3 - p._4)).sum / test.count.toDouble


    mae


  }

  //*********************************************************************************
  //********************** Measurements functions **********************************

  //computes five time measurements using the prediction method described by the argument f
  //returns min , max , mean and standard deviation of the measurements of both the total time to compute predictions and time to compute similarities
  def prediction_time_measures(f: () => (Long, Long, Double)): ((Long, Long, Double, Double), (Long, Long, Double, Double)) = {
    def helper(iter: Int, acc: List[(Long, Long)]): List[(Long, Long)] = {

      if (iter > 0) {
        val g = f()
        g match {
          case (predict_time: Long, similarity_time, s: Double) => helper(iter - 1, (predict_time, similarity_time) :: acc)
        }

      } else {
        acc
      }
    }

    val l = helper(5, Nil)

    // computing prediction time stats over the 5 runs
    val pred_times = l.map(_._1)
    val pred_time_mean = pred_times.reduce(_ + _) / l.length.toDouble
    val pred_time_stdev = math.sqrt(pred_times.map(r => (r - pred_time_mean) * (r - pred_time_mean)).reduce(_ + _) / l.length.toDouble)
    val pred_time_stats = (pred_times.min, pred_times.max, pred_time_mean, pred_time_stdev)

    // computing similarity computation time stats over the 5 runs
    val sim_times = l.map(_._2)
    val sim_time_mean = sim_times.reduce(_ + _) / l.length.toDouble
    val sim_time_stdev = math.sqrt(sim_times.map(r => (r - sim_time_mean) * (r - sim_time_mean)).reduce(_ + _) / l.length.toDouble)
    val sim_time_stats = (sim_times.min, sim_times.max, sim_time_mean, sim_time_stdev)

    (pred_time_stats, sim_time_stats)

  }


  //****************************************************************************
  //************ Main program ************************************

  val (train, test) = load_data()


  // For question 2.2.1
  val (_, _, cosineBasedMae) = predict_byCosineSimilarity()

  // For question 2.2.2
  val jaccardMae = predict_byJaccardSimilarity()


  // For question 2.2.3
  val number_of_users = train.map(_.user).distinct().count()
  val numberOfSimilarityComputations = (((number_of_users) * (number_of_users - 1)) / 2) + number_of_users


  // For question 2.2.4
  val multiplications_stats = compute_multiplications_needed(train)

  //For question 2.2.5 :
  val sim = compute_cosine_similarity()._1
  val n_bytes = sim.map(p => p._2.size).sum * 8


  // For question 2.2.6 and 2.2.7
  val (cosine_method_time_stats, similarity_compute_stats) = prediction_time_measures(predict_byCosineSimilarity)


  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach {
      f =>
        try {
          f.write(content)
        } finally {
          f.close
        }
    }

  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosineBasedMae, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosineBasedMae - 0.7669) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> jaccardMae, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (jaccardMae - cosineBasedMae) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> numberOfSimilarityComputations // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> multiplications_stats._1, // Datatype of answer: Double
              "max" -> multiplications_stats._2, // Datatype of answer: Double
              "average" -> multiplications_stats._3, // Datatype of answer: Double
              "stddev" -> multiplications_stats._4 // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> n_bytes // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> cosine_method_time_stats._1, // Datatype of answer: Double
              "max" -> cosine_method_time_stats._2, // Datatype of answer: Double
              "average" -> cosine_method_time_stats._3, // Datatype of answer: Double
              "stddev" -> cosine_method_time_stats._4 // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> similarity_compute_stats._1, // Datatype of answer: Double
              "max" -> similarity_compute_stats._2, // Datatype of answer: Double
              "average" -> similarity_compute_stats._3, // Datatype of answer: Double
              "stddev" -> similarity_compute_stats._4 // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> (similarity_compute_stats._3 / numberOfSimilarityComputations), // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> (similarity_compute_stats._3 / cosine_method_time_stats._3) // Datatype of answer: Double
          )
        )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
