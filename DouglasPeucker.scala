import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


case class Point(driverId: String, orderId: String, timeStamp: Int, longitude: Float, latitude: Float) extends Serializable

object DouglasPeucker extends DouglasPeucker {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("trajectory")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    val eps = 200.0

    val lines = sc.textFile("src/main/resources/sampleOf3drivers19orders.txt")
    val points = getPoints(lines)
    val routes = getRoutes(points)
    val compressedRoutes = compressRoutes(routes, eps)

    routes.collect().foreach(route => {
//      println(route._2)
      println(route._2.toArray.length)
    })

    compressedRoutes.collect().foreach(route => {
//      println(route._2.toIterable)
      println(route._2.length)
    })

//    lines.collect().foreach(line => println(line))
//    points.collect().foreach(line => reflect.io.File("src/main/resources/output/task2.txt").writeAll(line))
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


  def compressRoutes(routes: RDD[(String, Iterable[Point])], eps: Double): RDD[(String, Array[Point])] = {
    routes.map(route => {
      val routeId = route._1
      val routeArray = route._2.toArray
      val compressedRoute = douglasPeucker(0, routeArray.length-1, routeArray, eps)
      (routeId, compressedRoute)
    })
  }


  def distancePointToLine(p0: Point, p1: Point, p2: Point): Double = {
    val x0 = p0.longitude
    val y0 = p0.latitude
    val x1 = p1.longitude
    val y1 = p1.latitude
    val x2 = p2.longitude
    val y2 = p2.latitude

//    Math.abs(((y2-y1)*x0 - (x2-x1)*y0) + (x2*y1 - y2*x1)) / Math.sqrt(Math.pow(y2-y1, 2) + Math.pow(x2-x1, 2))

//    print("x0: ")
//    println(x0)
//    print("y0 ")
//    println(y0)
//    print("x1: ")
//    println(x1)
//    print("y1 ")
//    println(y1)
//    print("x2: ")
//    println(x2)
//    print("y2 ")
//    println(y2)
//
//
//    print("(y2-y1)*x0: ")
//    println((y2-y1)*x0)
//    print("(x2-x1)*y0: ")
//    println((x2-x1)*y0)
//    print("x2*y1: ")
//    println(x2*y1)
//    print("y2*x1: ")
//    println(y2*x1)
//    print("(y2-y1)*x0 - (x2-x1)*y0: ")
//    println((y2-y1)*x0 - (x2-x1)*y0)
//    print("x2*y1 - y2*x1: ")
//    println(x2*y1 - y2*x1)

    val tmp1 = (y2-y1)*x0 - (x2-x1)*y0
    val tmp2 = x2*y1 - y2*x1
    val up = Math.abs(tmp1 - tmp2)
    val down =  Math.sqrt(Math.pow(y2-y1, 2) + Math.pow(x2-x1, 2))
//    print("up: ")
//    println(up)
//    print("down: ")
//    println(down)
    up / down
  }


  def douglasPeucker(start: Int, end: Int, points: Array[Point], eps: Double): Array[Point] = {
    var results = Array[Point]()
//    if (end > start + 1) {
      var maxDist = -1.0
      var maxDistIndex = 0

      for ((middlePoint, i) <- points.view.zipWithIndex) {
        if (i > start && i < end) {
          val dist = distancePointToLine(middlePoint, points(start), points(end))
//          print("Dist: ")
//          println(dist)
          if (dist > maxDist) {
            maxDist = dist
            maxDistIndex = i
          }
        }
      }

//    print("MaxDist: ")
//    println(maxDist)
//    print("MaxDistIndex: ")
//    println(maxDistIndex)
//    println()

      if (maxDist > eps) {
        val resultList1 = douglasPeucker(start, maxDistIndex, points, eps)
        val resultList2 = douglasPeucker(maxDistIndex, end, points, eps)
        results ++= resultList1 ++ resultList2
      } else {
        results ++= Array(points(start), points(end))
      }
//    }
    results
  }
}
