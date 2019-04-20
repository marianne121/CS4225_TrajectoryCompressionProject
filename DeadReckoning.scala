import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

case class Point(longitude: Double, latitude: Double, timestamp: Double, velocity: Double, bearing: Double) extends Serializable

object DeadReckoning {
  def main(args: Array[String]) {
    // Spark config and context
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("DeadReckoning")
    val sc = new SparkContext(conf)

    val inputFile = sc.textFile("src/data/sampleOf3drivers19orders.txt")
    val raw     = rawPoints(inputFile)
    val groupedPointsByOrderID = groupPoints(raw)
    val sorted = sort(groupedPointsByOrderID)

    val processed = addVeloBearing(sorted)
    val compressed = deadReckoning(processed, 0.01)
    val out     = convertToOutput(compressed)
    // Input and Output files
    // Long,Lat,TimeStamp


    // Convert to a RDD and save to file
    out.saveAsTextFile("src/data/output")
  }

  /** Load points from the given file */
  def rawPoints(lines: RDD[String]): RDD[(String, Point)] =
    lines.map(line => {
      val arr = line.split(",")
      val s = arr(0) + arr(1)
      val p = Point(
        longitude =  arr(3).toDouble,
        latitude =   arr(4).toDouble,
        timestamp =  arr(2).toDouble,
        velocity =   0,
        bearing =    0
      )

      (s,p)
    })

  /** group by key */
  def groupPoints(points: RDD[(String,Point)]): RDD[(String, Iterable[Point])] =
    points.groupByKey()

  /** sort timestamps */
  def sort(points: RDD[(String, Iterable[Point])]): RDD[(String, Iterable[Point])] =
    points.map(x => {
      var arr = x._2.toArray
      arr = arr.sortBy(p => p.timestamp)
      (x._1, arr)
    })

  /** compress routes*/
  def addVeloBearing(points: RDD[(String, Iterable[Point])]): RDD[(String, Iterable[Point])] =
    points.map(x => {
      val arr = x._2.toArray
      val newArr = new Array[Point](arr.length)
      newArr(0) = arr(0)

      for (i <- 1 until arr.length) {
        val latx = arr(i-1).latitude
        val lonx = arr(i-1).longitude
        val timex = arr(i-1).timestamp
        val laty = arr(i).latitude
        val lony = arr(i).longitude
        val timey = arr(i).timestamp
        val bearing = getBearing(latx,lonx,laty,lony)
        val distance = getDistanceFromLatLonInKm(latx,lonx,laty,lony)
        val velocity = getVelocity(distance,timex,timey)

        val newPoint = Point(
          longitude =  arr(i).longitude,
          latitude =   arr(i).latitude,
          timestamp =  arr(i).timestamp,
          velocity =   velocity,
          bearing =    bearing
        )

        // Replace with new point
        newArr(i) = newPoint
      }
      (x._1, newArr)
    })

  def deadReckoning(points: RDD[(String, Iterable[Point])], errorThreshold: Double): RDD[(String, Iterable[Point])] =
    points.map(x => {
      val arr = x._2.toArray
      val newArr = new ArrayBuffer[Point]()
      newArr.append(arr(0))

      var currentPoint = arr(1)
      newArr.append(currentPoint)

      for (i <- 2 until arr.length) {
        val incomingPoint = arr(i)
        val timeDifference = incomingPoint.timestamp - currentPoint.timestamp

        val (newLat,newLong) = newPosition(currentPoint.latitude,currentPoint.longitude,currentPoint.velocity,currentPoint.bearing,timeDifference)
        val errorDistance = getDistanceFromLatLonInKm(incomingPoint.latitude,incomingPoint.longitude,newLat,newLong)

        // Add current point in only if error exceed threshold
        if (errorDistance > errorThreshold) {
          currentPoint = incomingPoint
          newArr.append(currentPoint)
        }
      }

      // Add last point if not added
      val lastTupleAdded = newArr.last
      val lastTupleInDF = arr.last
      if (lastTupleAdded.longitude == lastTupleInDF.longitude && lastTupleAdded.latitude == lastTupleInDF.latitude){
        newArr.append(lastTupleInDF)
      }

      val newArrComplete = newArr.toArray

      println("Compressed from " + arr.size + " points to " + newArrComplete.size)
      (x._1, newArrComplete)
    })

  def convertToOutput(points: RDD[(String, Iterable[Point])]): RDD[(String,Double,Double,Double)] =
    points.flatMapValues(x => x)
    .map(x => {
      val id = x._1
      val point = x._2

      (id, point.timestamp, point.longitude, point.latitude)
    })

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

  // Get bearing, 0 or 360 as north, go clockwise
  def getBearing(lat1: Double,long1: Double,lat2: Double,long2: Double): Double = {
    val dLon = (long2 - long1)

    val y = math.sin(dLon) * math.cos(lat2)
    val x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dLon)

    var brng = math.atan2(y, x)

    brng = math.toDegrees(brng)
    brng = (brng + 360) % 360
    brng = 360 - brng // count degrees clockwise - remove to make counter-clockwise
    if (brng == 360) {
      brng = 0
    }
    brng
  }

  // Velocity in km/s
  def getVelocity(dist_km: Double, time_start: Double, time_end: Double): Double = {
    val timeDiff = time_end - time_start
    if (timeDiff == 0) {
      0
    } else {
      dist_km / timeDiff
    }
  }

  def newPosition(lat: Double,lon: Double,velocity: Double,bearing: Double,seconds: Double): (Double,Double) = {
    val R = 6371  // Radius of the Earth

    val brng = math.toRadians(bearing) // Bearing is radians.
    val d = velocity * seconds

    val lat1 = math.toRadians(lat) // Current lat point converted to radians
    val lon1 = math.toRadians(lon) // Current long point converted to radians

    var lat2 = math.asin( math.sin(lat1)*math.cos(d/R) +
      math.cos(lat1)*math.sin(d/R)*math.cos(brng))

    var lon2 = lon1 + math.atan2(math.sin(brng)*math.sin(d/R)*math.cos(lat1),
      math.cos(d/R)-math.sin(lat1)*math.sin(lat2))

    lat2 = math.toDegrees(lat2)
    lon2 = math.toDegrees(lon2)

    (lat2, lon2)
  }





}