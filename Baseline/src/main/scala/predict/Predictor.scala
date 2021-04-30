package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.annotation.tailrec

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
  def load_data(): (RDD[Rating], RDD[Rating]) ={

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


  // compute the global average, returns the needed computation time and mae
  def calculate_globalAverage()={
    val (train, test)= load_data()

    val startTime= System.currentTimeMillis()
    val globalAv = train.map(r=> r.rating).mean
    val endTime= System.currentTimeMillis()
    val computation_time= (endTime - startTime) * 1000 // from milliseconds to microseconds

    val mae= test.map(r=> scala.math.abs(r.rating - globalAv)).reduce(_+_)/ test.count.toDouble

    (computation_time, mae)

  }


  // computes predictions using per user average method, returns computation time and mae
  def predict_ByUserAverage()={

    val (train, test)= load_data()
    val ratings_per_user = train.groupBy( _.user)
      .mapValues(l => l.map(_.rating).sum/ l.size)


    val g= test.map(r=> (r.user, r))
      .join(ratings_per_user)
      .map(r=> r._2)

    val startTime= System.currentTimeMillis()
    g.map(r=> r._2).reduce(_+_) // doing the sum to trigger the materialization of the RDD
    val endTime= System.currentTimeMillis()
    val computation_time= (endTime - startTime) * 1000 // from milliseconds to microseconds

    val mae= g.map(r=> scala.math.abs(r._1.rating - r._2)).reduce(_+_)/ g.count.toDouble

    (computation_time, mae)
  }

  // computes predictions using per item average method, returns computation time and mae
  def predict_ByItemAverage()={


    val (train, test)= load_data()
    val globalAv= train.map(r=> r.rating).mean
    val ratings_per_item = train.groupBy( _.item)
      .mapValues(l => l.map(_.rating).sum/ l.size)

    val g= test.map(r=> (r.item, r))
      .leftOuterJoin(ratings_per_item)
      .mapValues{
        case (s, None) => (s,globalAv)
        case (s, Some(r))=> (s,  r)}
      .map(_._2)

    val startTime= System.currentTimeMillis()
    g.map(r=> r._2).reduce(_+_) // doing the sum to trigger the materialization of the RDD
    val endTime= System.currentTimeMillis()

    val computation_time= (endTime - startTime) * 1000 // from milliseconds to microseconds

    val mae= g.map(r=> scala.math.abs(r._1.rating - r._2)).reduce(_+_)/ g.count.toDouble

    (computation_time, mae)
  }

  // computes predictions using baseline method, returns computation time and mae
  def predict_ByBaseline()={

    val (train, test)= load_data()
    val ratings_per_user = train.groupBy( _.user)
      .mapValues(l => l.map(_.rating).sum/ l.size)

    val deviaFromUserAverage= train.map(r=> (r.user, r))
      .join(ratings_per_user)
      .map(_._2)
      .map{case (x,y) => (x, normalize(x.rating, y))}

    // contains tuples (item, globalAverdeviation)
    val globAv_devia_per_item =deviaFromUserAverage.groupBy{case (r, deviation) => r.item}
      .mapValues(l => l.map(_._2).sum/ l.size)


    val g = test.map(r=> (r.user, r))
      .join(ratings_per_user)
      .map{ case (user, (r:Rating, userAverage)) => (r.item, (r, userAverage))}
      .leftOuterJoin(globAv_devia_per_item)
      .mapValues{
        case ((r, userAv),  None) => (r, userAv)
        case ((r, userAv),  Some(t))=> (r, userAv + t* scale(t+userAv, userAv)) }
      .map(_._2)

    val startTime= System.currentTimeMillis()
    g.map(r=> r._2).reduce(_+_) // doing the sum to trigger the materialization of the RDD
    val endTime= System.currentTimeMillis()

    val computation_time= (endTime - startTime) * 1000 // from milliseconds to microseconds

    val mae= g.map(r=> scala.math.abs(r._1.rating - r._2)).reduce(_+_)/ g.count.toDouble

    (computation_time, mae)

  }



  // compute the scale
  def scale (r: Double, av:Double): Double= {
    if (r > av) {
      5 - av
    } else if (r < av) {
      av - 1
    } else {
      1
    }
  }

  def normalize(x: Double, y:Double): Double={
    (x-y)/ scale(x,y)
  }




  //computes ten time measurements using the prediction method described by the argument f
  //returns min , max , mean and standard deviation of the measurements
  def time_measures(f: ()=> (Long, Double)): (Long, Long, Double, Double)  ={

    @tailrec
    def helper(iter: Int, acc: List[Long]): List[Long]= {

      if (iter > 0) {
        val g = f()
        g match {
          case (r: Long, s: Double) => helper(iter -1, r:: acc)
        }

      }else {
        acc
      }
    }

   val l= helper(10, Nil)
    val mean= l.reduce(_+_)/l.length.toDouble
    val stdev= math.sqrt(l.map(r=> (r- mean)*(r-mean)).reduce(_+_)/l.length.toDouble)

    (l.min, l.max, mean, stdev)

  }

  val (_, mae_globalAv)= calculate_globalAverage()
  val (_, mae_userAv)= predict_ByUserAverage()
  val (_, mae_itemAv)= predict_ByItemAverage()
  val (_, mae_baseline)= predict_ByBaseline()

  val time_measures_globalAv= time_measures(calculate_globalAverage)
  val time_measures_userAv= time_measures(predict_ByUserAverage)
  val time_measures_itemAv= time_measures(predict_ByItemAverage)
  val time_measures_baseline= time_measures(predict_ByBaseline)


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
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> mae_globalAv, // Datatype of answer: Double
              "MaePerUserMethod" -> mae_userAv, // Datatype of answer: Double
              "MaePerItemMethod" -> mae_itemAv, // Datatype of answer: Double
              "MaeBaselineMethod" -> mae_baseline // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> time_measures_globalAv._1 ,  // Datatype of answer: Double
                "max" -> time_measures_globalAv._2 ,  // Datatype of answer: Double
                "average" -> time_measures_globalAv._3 , // Datatype of answer: Double
                "stddev" -> time_measures_globalAv._4 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> time_measures_userAv._1,  // Datatype of answer: Double
                "max" -> time_measures_userAv._2,  // Datatype of answer: Double
                "average" -> time_measures_userAv._3, // Datatype of answer: Double
                "stddev" -> time_measures_userAv._4 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> time_measures_itemAv._1,  // Datatype of answer: Double
                "max" -> time_measures_itemAv._2,  // Datatype of answer: Double
                "average" -> time_measures_itemAv._3, // Datatype of answer: Double
                "stddev" -> time_measures_itemAv._4 // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> time_measures_baseline._1,  // Datatype of answer: Double
                "max" -> time_measures_baseline._2, // Datatype of answer: Double
                "average" -> time_measures_baseline._3, // Datatype of answer: Double
                "stddev" -> time_measures_baseline._4 // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> time_measures_baseline._3 / time_measures_globalAv._3 // Datatype of answer: Double
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
