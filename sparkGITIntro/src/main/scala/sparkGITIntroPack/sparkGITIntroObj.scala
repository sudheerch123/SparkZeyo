package sparkGITIntroPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object sparkGITIntroObj {
	def main(args:Array[String]): Unit=
		{
				val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
						val sc = new SparkContext(conf)
						sc.setLogLevel("ERROR")    
						val spark = SparkSession.builder().getOrCreate()
						import spark.implicits._
						println
						println("===============READ JSON USINF spark.read.json========================")
						println

						val jsondf = spark.read.json("file:///c:/data/devices.json")
						jsondf.show(5,false)
						jsondf.printSchema()
		}

}