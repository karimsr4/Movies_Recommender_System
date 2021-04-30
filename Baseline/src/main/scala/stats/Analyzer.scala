package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}


case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
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


  // compute Global average rating
  val globalAv = data.map(r => r.rating).mean

  //average rating for each user
  val ratings_per_user = data.groupBy( _.user)
    .mapValues(l => l.map(_.rating).sum/ l.size)

  // We compute here the min, max and average of the user average ratings
  val min_rating_per_user = ratings_per_user.values.min
  val max_rating_per_user = ratings_per_user.values.max
  val average_rating_per_user = ratings_per_user.values.mean

  // we keep only the users with average rating close to the global average rating
  val users_close_global_av = ratings_per_user.mapValues(x=> scala.math.abs(x- globalAv)).filter(_._2 < 0.5)
  // we compute the ratio
  val ratio_users = users_close_global_av.count.toDouble / ratings_per_user.count



  //average rating for each item
  val ratings_per_item = data.groupBy( _.item)
    .mapValues(l => l.map(_.rating).sum/ l.size)

  // We compute here the min, max and average of the item average ratings
  val min_rating_per_item = ratings_per_item.values.min
  val max_rating_per_item = ratings_per_item.values.max
  val average_rating_per_item = ratings_per_item.values.mean

  val items_close_global_av = ratings_per_item.mapValues(x=> scala.math.abs(x- globalAv)).filter(_._2 < 0.5)
  val ratio_items = items_close_global_av.count.toDouble / ratings_per_item.count


  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> globalAv  // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> min_rating_per_user ,  // Datatype of answer: Double
                "max" -> max_rating_per_user, // Datatype of answer: Double
                "average" -> average_rating_per_user // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> false, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratio_users  // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> min_rating_per_item,  // Datatype of answer: Double
                "max" -> max_rating_per_item, // Datatype of answer: Double
                "average" -> average_rating_per_item  // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> false, // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratio_items  // Datatype of answer: Double
          ),
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
