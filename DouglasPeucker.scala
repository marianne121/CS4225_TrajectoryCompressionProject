import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


case class Point(driverId: String, orderId: String, timeStamp: Int, longitude: Float, latitude: Float) extends Serializable

object DouglasPeucker extends DouglasPeucker {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("trajectory")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    val targetError = 0.001

    val lines = sc.textFile("src/main/resources/sampleOf3drivers19orders.txt")
    val points = getPoints(lines)
    val routes = getRoutes(points)
    val compressedRoutes = compressRoutes(routes, targetError)

    compressionResult(routes.collect(), compressedRoutes.collect())
  }
}

class DouglasPeucker extends Serializable {

  def getPoints(lines: RDD[String]): RDD[Point] =
    lines.map(line => {
      val arr = line.split(",")
      Point(
        driverId = arr(0),
        orderId = arr(1),
        timeStamp = arr(2).toInt,
        longitude = arr(3).toFloat,
        latitude = arr(4).toFloat
      )
    })


  def getRoutes(points: RDD[Point]): RDD[(String, Iterable[Point])] = {
    points.map(point => (point.orderId, point)).groupByKey()
  }


  def getDistanceFromLatLonInKm(lat1: Double,lon1: Double,lat2: Double,lon2: Double): Double = {
    val R = 6371 // Radius of the earth in km
    val dLat = math.toRadians(lat2-lat1)
    val dLon = math.toRadians(lon2-lon1)
    val rLat1 = math.toRadians(lat1)
    val rLat2 = math.toRadians(lat2)
    val a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(rLat1) * math.cos(rLat2) * math.sin(dLon/2) * math.sin(dLon/2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    val d = R * c  // Distance in km

    d
  }


  def distancePointToLineKm(p0: Point, p1: Point, p2: Point) : Double = {
    val x0 = p0.longitude
    val y0 = p0.latitude
    val x1 = p1.longitude
    val y1 = p1.latitude
    val x2 = p2.longitude
    val y2 = p2.latitude

    if (x1 == x2) {
      return getDistanceFromLatLonInKm(y0, x0, y0, x1)
    } else if (x1 == x2 && y1 == y2){
      return getDistanceFromLatLonInKm(y0, x0, y1, x1)
    }

    val m = (y2 - y1) / (x2 - x1)
    val c = y1 - (m*x1)
    val a = m
    val b = -1

    val r = (a*x0 + b*y0 + c) / (a*a + b*b) * -1
    val x3 = a*r + x0
    val y3 = b*r + y0

    if (x0 == x3 && y0 == y3) {
      return 0.0
    }
    getDistanceFromLatLonInKm(y0, x0, y3, x3)
  }


  def compressRoutes(routes: RDD[(String, Iterable[Point])], targetError: Double): RDD[(String, Array[Point], Double)] = {
    routes.map(route => {
      val routeId = route._1
      val routeArray = route._2.toArray
      var eps = 0.01
      var error = 1000.0
      var averageError = 1000.0
      var compressedRoute = Array[Point]()
      do {
        val (t1, t2) = douglasPeucker(0, routeArray.length-1, routeArray, eps)
        compressedRoute = t1
        error = t2
        averageError = error / compressedRoute.length

//        print("eps: ")
//        println(eps)
//        print("average error: ")
//        println(averageError)
//        println()

        eps = eps - 0.001
      } while (averageError > targetError)
      (routeId, compressedRoute, error)
    })
  }


  def distancePointToLine(p0: Point, p1: Point, p2: Point): Double = {
    val x0 = p0.longitude
    val y0 = p0.latitude
    val x1 = p1.longitude
    val y1 = p1.latitude
    val x2 = p2.longitude
    val y2 = p2.latitude

    val tmp1 = (y2-y1)*x0 - (x2-x1)*y0
    val tmp2 = x2*y1 - y2*x1
    val up = Math.abs(tmp1 + tmp2)
    val down =  Math.sqrt(Math.pow(y2-y1, 2) + Math.pow(x2-x1, 2))
    up / down
  }


  def douglasPeucker(start: Int, end: Int, points: Array[Point], eps: Double): (Array[Point], Double) = {
    var results = Array[Point]()
    var maxDist = -1.0
    var maxDistIndex = 0
    var error = 0.0

    for ((middlePoint, i) <- points.view.zipWithIndex) {
      if (i > start && i < end && !(points(start).longitude == points(end).longitude && points(start).latitude == points(end).latitude)) {
        val dist = distancePointToLineKm(middlePoint, points(start), points(end))
        error += dist
        if (dist > maxDist) {
          maxDist = dist
          maxDistIndex = i
        }
      }
    }

    if (maxDist > eps) {
      val (resultList1, error1) = douglasPeucker(start, maxDistIndex, points, eps)
      val (resultList2, error2) = douglasPeucker(maxDistIndex, end, points, eps)
      results ++= resultList1.dropRight(1) ++ resultList2
      error = error1 + error2
    } else {
      results ++= Array(points(start), points(end))
    }
    (results, error)
  }


  def compressionResult(original: Array[(String, Iterable[Point])], compressed: Array[(String, Array[Point], Double)]): Unit = {
    val combined = original zip compressed
    combined.foreach(tuple => {
      println(tuple._1._1)
      print("Average Error: ")
      println(tuple._2._3 / tuple._1._2.toArray.length.toFloat)
      print("Compression Ratio: ")
      println(tuple._2._2.length.toFloat / tuple._1._2.toArray.length.toFloat)
      println()
    })
  }
}
