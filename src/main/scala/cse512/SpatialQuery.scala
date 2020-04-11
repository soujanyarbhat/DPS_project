/*CSE 511 : DATA PROCESSING AT SCALE*/
/* Ankush Mandal 
Soujanya Ranganatha Bhat  
Tanmoy Purkait 
Raktim Mukhopadhyay  
Yash Vijay
Manas Mahapatra */

package cse512

import org.apache.spark.sql.SparkSession
import scala.math.sqrt
import scala.math.pow

object SpatialQuery extends App{
  def ST_Contains(queryRectangle:String, pointString:String):Boolean={
    val point = pointString.split(',').map(_.toDouble)
    val rectangle = queryRectangle.split(',').map(_.toDouble)

    val high_bound_x = math.max(rectangle(0), rectangle(2))
    val low_bound_x = math.min(rectangle(0), rectangle(2))
    val high_bound_y = math.max(rectangle(1), rectangle(3))
    val low_bound_y = math.min(rectangle(1), rectangle(3))

    val point_x = point(0)
    val point_y = point(1)

    if (point_x > high_bound_x || point_x < low_bound_x || point_y > high_bound_y || point_y < low_bound_y) return false
    else return true
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    /*Checking if the inputs are empty/null or distance is zero, then it will return False*/

    if (pointString1 == null || pointString1.isEmpty() || pointString2 == null || pointString2.isEmpty() || distance <= 0.00)
        return false

    val point1_coordinates = pointString1.split(",")
    var p1x = point1_coordinates(0).toDouble
    var p1y = point1_coordinates(1).toDouble

    val point2_coordinates = pointString2.split(",")
    var p2x = point2_coordinates(0).toDouble
    var p2y = point2_coordinates(1).toDouble

    // Distance between two points
    var euc_dist = sqrt(pow(p1x - p2x, 2) + pow(p1y - p2y, 2))
    if (euc_dist <= distance)
        return true
    else
        return false
}

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - DONE
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - DONE
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
