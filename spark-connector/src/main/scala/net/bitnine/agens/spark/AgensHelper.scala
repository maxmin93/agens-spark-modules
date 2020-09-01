package net.bitnine.agens.spark

import net.bitnine.agens.spark.AgensMeta

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.types.{DataType, StructField, StructType}


class AgensHelper

object AgensHelper {

	def hello = (msg:String) => s"Hello, $msg - AgensSparkConnector (since 2020-08-01)"

	// 1) ArrayType => MapType
	// 2) property.`type` => DataType
	// ==> select( col("id"), col("properties")("name").as("name!") )
	// https://medium.com/@mrpowers/working-with-spark-arraytype-and-maptype-columns-4d85f3c8b2b3

	// id, label, name$[type,valStr], age$
	// (key, type, valStr) => (key -> (type, valStr))
	// 3) df.withColumn("name",col("name$")[0].cast(col("name$")[1])	// ..cast(BooleanType))

	// **NOTE: UDF 에서는 Any 타입을 지원하지 않는다
	// https://stackoverflow.com/a/46274802

	// **ref https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/
	def convStr2Value(colName: String, convType:DataType)(df: DataFrame): DataFrame = {
		df.withColumn( colName+"$", col(colName).cast(convType) )
				.drop( col(colName) )
	}

	// ** transform steps:
	// 0) parameters : datasource, label(vertex/edge)
	// 1) explode properties( array of property )
	// 2) for loop : keysMeta
	//   2-1) select datasource, label, id, property from DF where property.key = keyMeta
	//	 2-2) convert property to column with datatype
	//	 2-3) append column and drop old column(= property)
	//	 2-4) left outer join
	// 3) output dataframe with appended all keys as field


	def explodeVertex(meta:AgensMeta)(df: DataFrame, dsName:String, lName:String): DataFrame = {
		import meta._
		val metaLabel = meta.datasource(dsName).label(lName)

		// STEP0: Base Dataframe
		var baseDf = df.select(
			col("datasource"),
			col("id"),
			col("label")
		)

		// STEP1: explode nested array field about properties
		val tmpDf = df.select(
			col("datasource"),
			col("id"),
			col("label"),
			explode(col("properties")).as("property")
		)

		// STEP2: convert property to column
		metaLabel.properties.values.foreach { p:meta.MetaProperty =>
			val df = tmpDf.filter(col("property.key") === p.name)
						.withColumn("tmp$", col("property.value"))
			val convDf = df.withColumn( p.name+"$", col("tmp$").cast(p.dataType()) )
					.drop( col("property") ).drop(col("tmp$"))
			baseDf = baseDf.join(convDf, Seq("datasource","label","id"),"left")
		}

		baseDf
	}

}
