package net.bitnine.agens.livy.jobs

import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession

class CypherJob  extends Job[java.lang.String] {

	override def call(jc: JobContext): java.lang.String = {

		val spark: SparkSession = jc.sparkSession()
		import spark.sqlContext.implicits._



		return ""
	}

}

/*

// Creating the string from an existing dataframe
val schema = df.schema
val jsonString = schema.json

// create a schema from json
import org.apache.spark.sql.types.{DataType, StructType}
val newSchema = DataType.fromJson(jsonString).asInstanceOf[StructType]

 */