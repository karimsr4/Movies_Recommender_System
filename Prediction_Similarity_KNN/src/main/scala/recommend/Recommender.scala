package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")


  val personalTrain = personalFile.map(l => {
    l.split(",").map(_.trim)
  }).filter(_.length == 3).map(r => Rating(944, r(0).toInt, r(2).toDouble))

  val train = data.union(personalTrain)

  val nonRatedMovieTitles = personalFile.map(l => {
    l.split(",").map(_.trim)
  }).filter(_.length != 3).map(r => (r(0).toInt, r(1)))

  val personalTest = personalFile.map(l => {
    l.split(",").map(_.trim)
  }).filter(_.length != 3).map(r => Rating(944, r(0).toInt, 0))


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


  //****************** Prediction by cosine method ****************************


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
      .filter { case (i, ((u1, s1), (u2, s2))) => u1 != u2 }
      .map { case (i, ((u1, s1), (u2, s2))) => ((u1, u2), s1 * s2) }
      .reduceByKey(_ + _)
      .map { case ((u1, u2), s12) => (u1, (u2, s12)) }
      .groupByKey()
      .cache()
      .mapValues(l => l.toMap)
      .collectAsMap()

    result

  }

  //*************************** Prediction **********************************
  def predict_byCosineSimilarity(sim: collection.Map[Int, Map[Int, Double]]): RDD[(Int, (Double, Double))] = {

    val transformed_dev = deviaFromUserAverage.map { case (r, dev) => (r.item, (r.user, dev)) }


    val predictions = personalTest.map(p => (p.item, (p.user, p.rating)))
      .leftOuterJoin(transformed_dev)
      .map { case (i, ((u, r), Some((v, dev)))) => ((u, i, r), (sim.get(u).get.getOrElse(v, 0.0) * dev, scala.math.abs(sim.get(u).get.getOrElse(v, 0.0))))
      case (i, ((u, r), None)) => ((u, i, r), (0.0, 1.0))
      }.reduceByKey((p, q) => (p._1 + q._1, p._2 + q._2))
      .mapValues { case (num, denom) => if (denom == 0) 0 else num / denom }
      .map { case ((u, i, r), s) => (u, (i, r, s)) }
      .join(ratings_per_user)
      .map { case (u, ((i, r, s), av)) => (u, i, r, av + s * scale(av + s, av)) }

    val rec = predictions.map(p => (p._2, (p._3, p._4)))

    rec

  }


  // take similarities needed
  // this function is used to take the top K largest similarities for each user from the "sim" map
  def takeKsim(k: Int, sim: collection.Map[Int, Map[Int, Double]]): collection.Map[Int, Map[Int, Double]] = {

    val new_sim = sim.map(p => (p._1, p._2.toList.sortBy(-_._2).take(k).toMap))

    new_sim


  }

  // ************************* Main program **********************


  val ratings_per_user = train.groupBy(_.user)
    .mapValues(l => l.map(_.rating).sum / l.size).cache()

  val deviaFromUserAverage = train.map(r => (r.user, r))
    .join(ratings_per_user)
    .map(_._2)
    .map { case (x, y) => (x, normalize(x.rating, y)) }.cache()

  val preprocessed = preprocess(train, ratings_per_user, deviaFromUserAverage)

  val sim = compute_cosine_similarity(preprocessed)
  val sim300 = takeKsim(300, sim)

  val resultsK30 = predict_byCosineSimilarity(takeKsim(30, sim300))
  val rec30 = resultsK30.map { case (i, (r, pred)) => (i, pred) }
    .join(nonRatedMovieTitles)
    .map { case (movieId, (pred, movieTitle)) => (movieId, movieTitle, pred) }
    .sortBy(r => (r._3, -r._1), ascending = false)

  val top5WithK30 = rec30.take(5)

  val resultsK300 = predict_byCosineSimilarity(sim300)
  val rec300 = resultsK300.map { case (i, (r, pred)) => (i, pred) }
    .join(nonRatedMovieTitles)
    .map { case (movieId, (pred, movieTitle)) => (movieId, movieTitle, pred) }
    .sortBy(r => (r._3, -r._1), ascending = false)

  val top5WithK300 = rec300.take(5)


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

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q3.2.5" -> Map(
            "Top5WithK=30" ->
              top5WithK30.map { case (movieId, movieTitle, pred) => List(movieId, movieTitle, pred) }.toList,

            "Top5WithK=300" ->
              top5WithK300.map { case (movieId, movieTitle, pred) => List(movieId, movieTitle, pred) }.toList

            // Discuss the differences in rating depending on value of k in the report.
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
