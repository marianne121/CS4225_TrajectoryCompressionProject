package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
        val distances = calculateDistances(raw)
        sc.parallelize(distances.collect()).repartition(1).saveAsTextFile(output)

    }

    /** Load locations from given file*/
    def rawLocations(lines: RDD[String]): RDD[Location] = lines.map(line => {
            val arr = line.split(",")
            Location(longitude = arr(0).toDouble,
                latitude = arr(1).toDouble,
                timeStamp = arr(2).toDouble)
    })

    def calculateDistances(locations: RDD[Location]) : RDD[Double] = {
        val anchor = locations.first()
        val remaining = locations.mapPartitionsWithIndex{
            case (index, iterator) => if(index==0) iterator.drop(1) else iterator
        }
        val remaining2 = remaining.mapPartitionsWithIndex{
            case (index, iterator) => if(index==0) iterator.drop(1) else iterator
        }
        val floater = remaining2.first()
        print(floater)

        val locationMap = locations.map(Location => Location)
        // remove first point because it is the float
        locations.map(Location => findDistance(anchor, Location, floater))
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

    def findDistance(anchor: Location, point: Location, floater: Location) : Double = {
        val grad = findGradient(anchor, floater)
        val intercept = findIntercept(anchor, grad)
        val distance = Math.abs(grad*point.longitude + (-1)*point.latitude + intercept) / Math.sqrt((grad*grad) + (-1)*(-1))
        println(distance)
        distance
    }





}
