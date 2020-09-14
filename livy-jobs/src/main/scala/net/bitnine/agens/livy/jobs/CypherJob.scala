package net.bitnine.agens.livy.jobs

import net.bitnine.agens.livy.AgensHiveConfig
import net.bitnine.agens.livy.ExecuteCypher.convertURI
import net.bitnine.agens.spark.Agens
import net.bitnine.agens.spark.avro.SchemaConverters
import org.apache.avro.SchemaBuilder
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession

class CypherJob(parameters: java.util.Map[java.lang.String,java.lang.String]) extends Job[java.lang.String] {

	val datasource = parameters.get(AgensHiveConfig.DATASOURCE.fullName)
	val name = parameters.get(AgensHiveConfig.NAME.fullName)
	val query = parameters.get(AgensHiveConfig.QUERY.fullName)

	override def call(jc: JobContext): java.lang.String = {

		val spark: SparkSession = jc.sparkSession()
		import spark.sqlContext.implicits._

		val agens:Agens = AgensBuilder
				.session(spark)
				.datasource(datasource)
				// agens.warehouse.path		// .warehouse("/user/agens/temp")

		val result = agens.cypher(query)
		val df = result.saveAsAvro(name)

		val dataSchema = df.schema
		val build = SchemaBuilder.record(name).namespace(SchemaConverters.AGENS_AVRO_NAMESPACE)
		val avroSchema = SchemaConverters.convertStructToAvro(dataSchema, build, SchemaConverters.AGENS_AVRO_NAMESPACE)

		avroSchema.toString
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