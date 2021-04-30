package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
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
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")

  // *************************************************************************
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

  // ******************** Similarity computation *****************************


  def preprocess(data: RDD[Rating], ratings_per_user: RDD[(Int, Double)], deviaFromUserAverage: RDD[(Rating, Double)]): RDD[(Int, Int, Double)] = {


    val denom = deviaFromUserAverage.groupBy { case (r: Rating, dev) => r.user }
      .mapValues(l => math.sqrt(l.map { case (r, dev) => dev * dev }.sum))


    val preprocessed = deviaFromUserAverage.map { case (r, dev) => (r.user, (r, dev)) }
      .join(denom)
      .mapValues { case ((r, dev), denominator) => if (denominator == 0) (r, 0.0) else (r, dev / denominator) }
      .map(_._2)

    preprocessed.map { case (r, preprocess) => (r.user, r.item, preprocess) }

  }

  // computes all similarities, return a Map of u1 -> Map(u2, similarity_1_2), self similarity not included
  def compute_cosine_similarity(preprocessed: RDD[(Int, Int, Double)]): collection.Map[Int, Map[Int, Double]] = {

    val preprocessed_new = preprocessed.map { case (u, i, score) => (i, (u, score)) }
    val result = preprocessed_new.join(preprocessed_new)
      .filter { case (i, ((u1, s1), (u2, s2))) => u1 != u2 } // we do not include self similarity
      .map { case (i, ((u1, s1), (u2, s2))) => ((u1, u2), s1 * s2) }
      .reduceByKey(_ + _)
      .map { case ((u1, u2), s12) => (u1, (u2, s12)) }
      .groupByKey()
      .cache()
      .mapValues(l => l.toMap)
      .collectAsMap()

    result

  }


  //**************************************************************************

  //*************************** Prediction **********************************
  def predict_byCosineSimilarity(sim: collection.Map[Int, Map[Int, Double]]): Double = {


    val transformed_dev = deviaFromUserAverage.map { case (r, dev) => (r.item, (r.user, dev)) }


    val predictions = test.map(p => (p.item, (p.user, p.rating)))
      .leftOuterJoin(transformed_dev)
      .map { case (i, ((u, r), Some((v, dev)))) => ((u, i, r), (sim.get(u).get.getOrElse(v, 0.0) * dev, scala.math.abs(sim.get(u).get.getOrElse(v, 0.0))))
      case (i, ((u, r), None)) => ((u, i, r), (0.0, 1.0))
      }.reduceByKey((p, q) => (p._1 + q._1, p._2 + q._2))
      .mapValues { case (num, denom) => if (denom == 0) 0 else num / denom }
      .map { case ((u, i, r), s) => (u, (i, r, s)) }
      .join(ratings_per_user)
      .map { case (u, ((i, r, s), av)) => (u, i, r, av + s * scale(av + s, av)) }

    predictions.map(p => scala.math.abs(p._3 - p._4)).reduce(_ + _)


    val mae = predictions.map(p => scala.math.abs(p._3 - p._4)).sum / test.count.toDouble

    mae

  }


  // take similarities needed
  // this function is used to take the top K largest similarities for each user from the "sim" map
  def takeKsim(k: Int, sim: collection.Map[Int, Map[Int, Double]]): collection.Map[Int, Map[Int, Double]] = {

    val new_sim = sim.map(p => (p._1, p._2.toList.sortBy(-_._2).take(k).toMap))
    new_sim
  }


  //**************************** Main program **************************

  val baseLineMae = 0.7669

  val ratings_per_user = train.groupBy(_.user)
    .mapValues(l => l.map(_.rating).sum / l.size).cache()

  val deviaFromUserAverage = train.map(r => (r.user, r))
    .join(ratings_per_user)
    .map(_._2)
    .map { case (x, y) => (x, normalize(x.rating, y)) }.cache()

  val preprocessed = preprocess(train, ratings_per_user, deviaFromUserAverage)

  //computing similarities with the biggest k i.e k=943
  val sim = takeKsim(943, compute_cosine_similarity(preprocessed))


  val sim10 = takeKsim(10, sim)
  val maeForK10 = predict_byCosineSimilarity(sim10)

  val sim30 = takeKsim(30, sim)
  val maeForK30 = predict_byCosineSimilarity(sim30)

  val sim50 = takeKsim(50, sim)
  val maeForK50 = predict_byCosineSimilarity(sim50)

  val sim100 = takeKsim(100, sim)
  val maeForK100 = predict_byCosineSimilarity(sim100)

  val sim200 = takeKsim(200, sim)
  val maeForK200 = predict_byCosineSimilarity(sim200)

  val sim300 = takeKsim(300, sim)
  val maeForK300 = predict_byCosineSimilarity(sim300)

  val sim400 = takeKsim(400, sim)
  val maeForK400 = predict_byCosineSimilarity(sim400)

  val sim800 = takeKsim(800, sim)
  val maeForK800 = predict_byCosineSimilarity(sim800)

  val maeForK943 = predict_byCosineSimilarity(sim)


  //***************************************************************************

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
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> maeForK10, // Datatype of answer: Double
            "MaeForK=30" -> maeForK30, // Datatype of answer: Double
            "MaeForK=50" -> maeForK50, // Datatype of answer: Double
            "MaeForK=100" -> maeForK100, // Datatype of answer: Double
            "MaeForK=200" -> maeForK200, // Datatype of answer: Double
            "MaeForK=300" -> maeForK300, // Datatype of answer: Double
            "MaeForK=400" -> maeForK400, // Datatype of answer: Double
            "MaeForK=800" -> maeForK800, // Datatype of answer: Double
            "MaeForK=943" -> maeForK943, // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> 100, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> (maeForK100 - baseLineMae) // Datatype of answer: Double
          ),

          "Q3.2.2" -> Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> sim10.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> sim30.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> sim50.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> sim100.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> sim200.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> sim300.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> sim400.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> sim800.map(_._2.size).sum * 8, // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> sim.map(_._2.size).sum * 8 // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> 8589934592L, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> scala.math.floor(8589934592L / (3 * 100 * 8)).toLong // Datatype of answer: Long
          )

          // Answer the Question 3.2.4 exclusively on the report.
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
