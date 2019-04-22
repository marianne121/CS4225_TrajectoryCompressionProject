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

        val output = "data/output6.csv"
        val header = lines.first() // extract header
        val linesWithoutHeader = lines.filter(row => row != header) // filter out header
        val raw = rawLocations(linesWithoutHeader)

        val sorted = raw.sortBy(Location => Location.timeStamp)
        val compressed = findCompressed(sorted)

        val compressedRdd = sc.parallelize(compressed)
        sc.parallelize(compressedRdd.collect()).repartition(1).saveAsTextFile(output)

    }


    /** Load locations from given file*/
    def rawLocations(lines: RDD[String]): RDD[Location] = lines.map(line => {
            val arr = line.split(",")
        // To implement separate by order id
            Location(longitude = arr(2).toDouble,
                latitude = arr(3).toDouble,
                timeStamp = arr(4).toDouble)
    })

    def findCompressed(locations: RDD[Location]) : ArrayBuffer[Location] = {
        var y = 0
        var compressed = ArrayBuffer[Location]() // create a copy of location rdd as an array buffer
        // compressed keeps the compressed route
        val locationArray = locations.collect() // convert location rdd to array

        println("initial  size")
        println(locations.count())

        // add first anchor to compressed
        compressed += locationArray(0)
        var remaining = locationArray
//        var remaining = locations.mapPartitionsWithIndex{
//            case (index, iterator) => if(index==0) iterator.drop(1) else iterator
//        }
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
                   // remaining = locations.subtract(compressedRdd).sortBy(_.timeStamp)
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
        compressed
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
    def findIntercept(anchor: Location, grad: Double) : Double = {
        val coordX = anchor.longitude
        val coordY = anchor.latitude
        val intercept = coordY - (grad * coordX)
        println("intercept is " + intercept)
        intercept
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





}
