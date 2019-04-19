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

        val lines   = sc.textFile("data/test.csv")
        val output = "data/output.csv"
        val header = lines.first() // extract header
        val linesWithoutHeader = lines.filter(row => row != header) // filter out header
        val raw = rawLocations(linesWithoutHeader)
        //val rawWithIndex = raw.zipWithIndex()
        //val indexKey = rawWithIndex.map{case (k,v) => (v,k)}
        val compressed = findCompressed(raw)
        val compressedRdd = sc.parallelize(compressed)
        sc.parallelize(compressedRdd.collect()).repartition(1).saveAsTextFile(output)

    }

    /** Load locations from given file*/
    def rawLocations(lines: RDD[String]): RDD[Location] = lines.map(line => {
            val arr = line.split(",")
            Location(longitude = arr(0).toDouble,
                latitude = arr(1).toDouble,
                timeStamp = arr(2).toDouble)
    })

    def findCompressed(locations: RDD[Location]) : ArrayBuffer[Location] = {
        var y = 0
        var compressed = ArrayBuffer[Location]()

        compressed += locations.first()
        var remaining = locations.mapPartitionsWithIndex{
            case (index, iterator) => if(index==0) iterator.drop(1) else iterator
        }
        while (y < locations.count()) {
            val anchor = compressed.last
            // remove everything until new anchor

            var i = 0
            while(i < remaining.count()-1) {
                val points = remaining.take(2+i)
                println("Over here")
                points.foreach(Location => println(Location))
                val result = findNewAnchor(anchor,points)
                println("new anchor index")
                println(result._1)
                if(result._2 != -1) {
                    // add new anchor to compressed list
                    println("add this point")
                    println(result._1)
                    compressed += result._1
                    i = result._2
                    i = remaining.count().toInt
                } else {
                    i = i+1
                }
            }
            y = y + i
        }
        compressed
    }

    /** Given 2 points, find the gradient of the line formed */
    def findGradient(anchor: Location, point: Location) : Double = {
        val coordOneX = anchor.longitude
        val coordOneY = anchor.latitude
        val coordTwoX = point.longitude
        val coordTwoY = point.latitude
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
        val threshold = 5.0*Math.pow(10,-8)
        val floater = points.last
        val grad = findGradient(anchor, floater)
        val intercept = findIntercept(anchor, grad)
        var i =0

        while(i < points.length) {
            val distance = Math.abs(grad*points(i).longitude + (-1)*points(i).latitude + intercept) /
                Math.sqrt((grad*grad) + (-1)*(-1))
            if(distance > threshold) {
                println("HERE IS THE DISTANCE")
                println(distance)
                println(threshold)
                return (points(i),i)
            }
            i = i+1
        }
        (anchor,-1) // anchor remains the same as all points are within expected threshold

    }





}
