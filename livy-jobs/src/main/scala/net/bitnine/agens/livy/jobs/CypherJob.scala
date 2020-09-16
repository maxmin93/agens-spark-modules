package net.bitnine.agens.livy.jobs

import net.bitnine.agens.livy.AgensHiveConfig
import net.bitnine.agens.spark.Agens.ResultsAsDF
import net.bitnine.agens.spark.{Agens, AgensBuilder}
import net.bitnine.agens.spark.avro.SchemaConverters
import org.apache.avro.SchemaBuilder
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.PropertyGraph

class CypherJob(parameters: java.util.Map[java.lang.String,java.lang.String]) extends Job[java.lang.String] {

	val datasource = parameters.get(AgensHiveConfig.DATASOURCE.fullName)
	val name = parameters.get(AgensHiveConfig.NAME.fullName)
	val query = parameters.get(AgensHiveConfig.QUERY.fullName)

	override def call(jc: JobContext): java.lang.String = {

		val spark: SparkSession = jc.sparkSession()
		println("** parameters")
		println(s"- datasource: $datasource")
		println(s"- datasource: $datasource")
		println(s"- datasource: $datasource")
		//////////////////////////////////
		// 1) spark-connector : connect to elasticsearch

		// **NOTE: AgensConf must be set by spark-default.conf
		val agens:Agens = AgensBuilder(spark)
				.host("minmac")
				.port("29200")
				.user("elastic")
				.password("bitnine")
				.vertexIndex("agensvertex")
				.edgeIndex("agensedge")
				.build

		//////////////////////////////////
		// 2) spark-cypher : run query

		val graph:PropertyGraph = agens.graph(datasource)

		// query: {name, age}, {name, country}
		println("\n** query => "+query)
		val result = graph.cypher(query)
		result.show

		// save to '/user/agens/temp' as avro
		println("\n** tempPath ==> "+ agens.conf.tempPath)
		agens.saveResultAsAvro(result, name)

		//////////////////////////////////
		// 3) convert schema of result to avro schema

		val df = result.asDataFrame
		val build = SchemaBuilder.record(name).namespace(SchemaConverters.AGENS_AVRO_NAMESPACE)
		val avroSchema = SchemaConverters.convertStructToAvro(df.schema, build, SchemaConverters.AGENS_AVRO_NAMESPACE)

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