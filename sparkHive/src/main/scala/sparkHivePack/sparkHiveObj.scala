package sparkHivePack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object sparkHiveObj {

	def main(args:Array[String]):Unit = {
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val txndf = spark.read.format("csv").options(Map("header"->"true","inferSchema"->"true"))
					.load("file:///c:/data/txns_with_header.txt")

					txndf.createOrReplaceTempView("txns_data")
					val vardf = spark.sql("select max(txnno) as txnno from txns_data ")
					vardf.show()
					vardf.printSchema()

					val var1 = vardf.select("txnno").collect().mkString("")
					println(s"Variable stores - $var1")

					/*
					 * 
					TASK -1 AND TASK -2				  

					  val nulldf = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///c:/data/nulldata.txt")

					println("==============NULLDATA.TXT  SCHEMA AND RAW DATA==========")
					nulldf.printSchema()
					nulldf.show()
					println("==============REPLACE STRING NULLs==========")
					val strdf = nulldf.na.fill("NA")
					strdf.printSchema()
					strdf.show()
					println("==============REPLACE INTEGER NULLs==========")

					val intdf = strdf.na.fill(0)
					intdf.printSchema()
					intdf.show()
println("==============Adding Date Column to above DataFrame==========")
intdf.withColumn("date", current_date()).show()*/

	}

}