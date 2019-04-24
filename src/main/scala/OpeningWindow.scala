package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class Location(longitude: Double, latitude: Double, timeStamp: Double)

object OpeningWindow {
    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ObjectTrajectory")
    @transient lazy val sc: SparkContext = new SparkContext(conf)
    /** Main function */
    def main(args: Array[String]): Unit = {

        val lines   = sc.textFile("data/separateOrders/order6")

        val output = "data/output.csv"
        val header = lines.first() // extract header
        val linesWithoutHeader = lines.filter(row => row != header) // filter out header
        val raw = rawLocations(linesWithoutHeader)
        val groupedPointsByOrderID = groupPoints(raw)

        val sorted = sort(groupedPointsByOrderID)
        val compressed =
        // comment to test new distance functions
//        val compressed = sorted.map(k => (k._1, findCompressed(k._2)))

        val out = convertToOutput(compressed)
        out.saveAsTextFile(output)

    }


    /** Load locations from given file*/
    def rawLocations(lines: RDD[String]): RDD[(String, Location)] = lines.map(line => {
            val arr = line.split(",")
        // To implement separate by order id
            val s = arr(0) + arr(1)
            val l = Location(longitude = arr(2).toDouble,
                latitude = arr(3).toDouble,
                timeStamp = arr(4).toDouble)
        (s,l)
    })

    def findCompressed(locations: Iterable[Location]) : Iterable[Location] = {
       var y = 0
        var compressed = ArrayBuffer[Location]() // create a copy of location rdd as an array buffer
        // compressed keeps the compressed route
        //Todo: line timesout
        val locationArray = locations.toArray // convert location rdd to array

        println("initial  size")
        println(locations.size)

        // add first anchor to compressed
        compressed += locationArray(0)
        var remaining = locationArray
        while (y < locationArray.length) {
            val anchor = compressed.last
            val anchorIndex = remaining.indexOf(anchor)
            remaining = remaining.drop(anchorIndex+1)
            println("anchor index is")
            println(anchorIndex)
            // remove everything until new anchor
            println("current size remaining")
            println(remaining.length)
            //println(remaining.deep.mkString("\n"))
            var i = 0
            var lastProcessed = anchor
            while(i < remaining.length-1) {
                val points = remaining.take(2+i)
                println("current anchor")
                println(anchor)
                println("Over here")
                points.foreach(Location => println(Location))
                val result = findNewAnchor(anchor,points)
                println("new anchor index")
                println(result._2)
                if(result._2 != -1) {
                    // add new anchor to compressed list
                    println("add this point")
                    println(result._1)
                    compressed += result._1
                    // moment it is broken, force exit loop
                    i = remaining.length
                    lastProcessed = result._1
                } else {
                    // if no new anchor found, last processed is the end of buffer region
                    lastProcessed = remaining(remaining.length-1)
                }

                i = i+1
            }
            // start of sliding window
            y = locationArray.indexOf(lastProcessed) + 1
            println("y is")
            println(y)
            val lastElement = locationArray(locationArray.length-1)
            if (remaining.length==1 && !compressed.contains(lastElement)) {
                // add in last point
                compressed += lastElement
                y = y +1
            }
        }
        compressed.toIterable
    }

    /** Given 2 points, find the gradient of the line formed */
    def findGradient(anchor: Location, point: Location) : Double = {
        val coordOneX = anchor.longitude
        val coordOneY = anchor.latitude
        val coordTwoX = point.longitude
        val coordTwoY = point.latitude
        if (coordOneX - coordTwoX == 0) { // gradient is infinity, vertical line
            return -1
        }
        val gradient = (coordOneY - coordTwoY) / (coordOneX - coordTwoX)
        println("gradient is " + gradient)
        gradient
    }

    /** Given a point and the gradient, find the intercept of the line */
    def findIntercept(point: Location, grad: Double) : Double = {
        val coordX = point.longitude
        val coordY = point.latitude
        val intercept = coordY - (grad * coordX)
        println("intercept is " + intercept)
        intercept
    }

    def findPerpendicular(gradient: Double, floater: Location) : (Double, Double) = {
        val perpendicularGradient = -1*gradient
        val intercept = findIntercept(floater, perpendicularGradient)
        // y = perpendicularGradient (x) + intercept
        (perpendicularGradient, intercept)
    }

    def findPointIntersect(firstGrad: Double, secondGrad: Double, firstIntercept: Double,
                               secondIntercept: Double): (Double, Double) = {
        val xCoordinate = (firstIntercept - secondIntercept) / (firstGrad - secondGrad)
        val yCoordinate = firstGrad * xCoordinate + firstIntercept
        (xCoordinate, yCoordinate)
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

    def findNewAnchorTest(anchor: Location, points: Array[Location]) : () = {
        //val threshold = 1.0*Math.pow(10,-4)
        val floater = points.last
        val grad = findGradient(anchor, floater)
        val intercept = findIntercept(anchor, grad)
        var i = 0

        while(i < points.length) {
            var distance = 0.toDouble
            val perpendicularEqn = findPerpendicular(grad, points(i))
            val intersectPoint = findPointIntersect(grad, perpendicularEqn._1, intercept, perpendicularEqn._2)
            //exceptions: gradient = -1 (infinity) --> vertical line or gradient = 0 --> horizontal line
            distance = getDistanceFromLatLonInKm(anchor.latitude, anchor.longitude, points(i).latitude, points(i).longitude)

            println("HERE IS THE DISTANCE")
            println(distance)
//            println(threshold)
//            if(distance > threshold) {
//                println("distance > threshold")
//                // return the point just before the float as the new anchor
//                return (points(points.length-2),i)
//            }
            i = i+1
        }

    }

    def findNewAnchor(anchor: Location, points: Array[Location]) : (Location, Int) = {
        val threshold = 1.0*Math.pow(10,-4)
        val floater = points.last
        val grad = findGradient(anchor, floater)
        val intercept = findIntercept(anchor, grad)

        var i =0
        println("length of buffer")
        println(points.length)

        while(i < points.length) {
            var distance = 0.toDouble
            if(grad != -1) {
                distance = Math.abs(grad*points(i).longitude + (-1)*points(i).latitude + intercept) /
                    Math.sqrt((grad*grad) + (-1)*(-1))
            } else { // vertical line
                distance = Math.abs(points(i).longitude - anchor.longitude)
            }

            println("HERE IS THE DISTANCE")
            println(distance)
            println(threshold)
            if(distance > threshold) {
                println("distance > threshold")
                // return the point just before the float as the new anchor
                return (points(points.length-2),i)
            }
            i = i+1
        }
        (anchor,-1) // anchor remains the same as all points are within expected threshold

    }

    /** group by orderId */
    def groupPoints(points: RDD[(String,Location)]): RDD[(String, Iterable[Location])] =
        points.groupByKey()

    /** sort timestamps */
    def sort(points: RDD[(String, Iterable[Location])]): RDD[(String, Iterable[Location])] =
        points.map(x => {
            var arr = x._2.toArray
            arr = arr.sortBy(p => p.timeStamp)
            (x._1, arr)
        })

    def convertToOutput(points: RDD[(String, Iterable[Location])]): RDD[(String,Double,Double,Double)] =
        points.flatMapValues(x => x)
            .map(x => {
                val id = x._1
                val point = x._2

                (id, point.timeStamp, point.longitude, point.latitude)
            })

}
