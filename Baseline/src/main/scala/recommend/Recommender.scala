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


  def predict_ByBaseline() = {


    val ratings_per_user = train.groupBy(_.user)
      .mapValues(l => l.map(_.rating).sum / l.size)

    val deviaFromUserAverage = train.map(r => (r.user, r))
      .join(ratings_per_user)
      .map(_._2)
      .map { case (x, y) => (x, normalize(x.rating, y)) }

    // contains tuples (item, globalAverdeviation)
    val globAv_devia_per_item = deviaFromUserAverage.groupBy { case (r, deviation) => r.item }
      .mapValues(l => l.map(_._2).sum / l.size)


    val g = personalTest.map(r => (r.user, r))
      .join(ratings_per_user)
      .map { case (user, (r: Rating, userAverage)) => (r.item, (r, userAverage)) }
      .leftOuterJoin(globAv_devia_per_item)
      .mapValues {
        case ((r, userAv), None) => (r, userAv)
        case ((r, userAv), Some(t)) => (r, userAv + t * scale(t + userAv, userAv))
      }
      .map(_._2)

    g

  }

  val results = predict_ByBaseline()
  val rec = results.map { case (r, pred) => (r.item, pred) }
    .join(nonRatedMovieTitles)
    .map { case (movieId, (pred, movieTitle)) => (movieId, movieTitle, pred) }
    .sortBy(r => (r._3, - r._1), ascending = false)

  val top = rec.take(5)


  // prediction function used for bonus question
  def predict_ByBaseline_popularity() = {


    val ratings_per_user = train.groupBy(_.user)
      .mapValues(l => l.map(_.rating).sum / l.size)

    val number_of_rating_per_item = train.groupBy(_.item).mapValues(l => l.size)
    val max_popularity = number_of_rating_per_item.values.max


    val deviaFromUserAverage = train.map(r => (r.user, r))
      .join(ratings_per_user)
      .map(_._2)
      .map { case (x, y) => (x, normalize(x.rating, y)) }

    // contains tuples (item, globalAverdeviation)
    val globAv_devia_per_item = deviaFromUserAverage.groupBy { case (r, deviation) => r.item }
      .mapValues(l => l.map(_._2).sum / l.size)


    val g = personalTest.map(r => (r.user, r))
      .join(ratings_per_user)
      .map { case (user, (r: Rating, userAverage)) => (r.item, (r, userAverage)) }
      .leftOuterJoin(globAv_devia_per_item)
      .mapValues {
        case ((r, userAv), None) => (r, userAv)
        case ((r, userAv), Some(t)) => (r, userAv + t * scale(t + userAv, userAv))
      }.leftOuterJoin(number_of_rating_per_item)
      .mapValues {
        case ((r, pred), None) => (r, pred - 1 )
        case ((r, pred), Some(t)) => (r, pred - 0.5 + scala.math.tanh((t/max_popularity.toDouble)* t))
      }.mapValues{ case (r, pred)=> (r,  if (pred <1) 1 else if (pred >5) 5 else pred)}
      .map(_._2)

    g

  }



  // computing the top recommendation for bonus question
  val results_with_popularity = predict_ByBaseline_popularity()
  val rec_with_popularity = results_with_popularity.map { case (r, pred) => (r.item, pred) }
    .join(nonRatedMovieTitles)
    .map { case (movieId, (pred, movieTitle)) => (movieId, movieTitle, pred) }
    .sortBy(r => (r._3, - r._1), ascending = false)

  val top_with_popularity = rec_with_popularity.take(5)

  println(top_with_popularity.toList)


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

          "Q4.1.1" -> top.map{case (movieId, movieTitle, pred) => List(movieId, movieTitle, pred)}.toList,
          "Q4.1.2" -> top_with_popularity.map{case (movieId, movieTitle, pred) => List(movieId, movieTitle, pred)}.toList
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
