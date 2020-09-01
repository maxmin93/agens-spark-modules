package net.bitnine.agens.spark

import net.bitnine.agens.spark.Agens.{schemaE, schemaV}
import net.bitnine.agens.spark.AgensMeta
import net.bitnine.agens.cypher.api.CAPSSession

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, count, explode, lit}
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession, functions}

import scala.language.implicitConversions


object Agens extends Serializable {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	implicit def apply(sc: SparkContext): Agens = {
		new Agens(sc)
	}

//	case class ElasticProperty(key:String, `type`:String, value:String)
//	case class ElasticVertex(timestamp:String, datasource:String, id:String, label:String
//							, properties:Array[ElasticProperty])
//	case class ElasticEdge(timestamp:String, datasource:String, id:String, label:String
//							, src:String, dst:String
//							, properties:Array[ElasticProperty])
//	case class ElasticFragment(datasource:String, id:String, label:String, property:ElasticProperty)

//	val encoderP = RowEncoder(schemaP)

	val schemaP = StructType( Array(
		StructField("key", StringType, false),
		StructField("type", StringType, false),
		StructField("value", StringType, false)
	))

	// ** Equal to ==> Encoders.product[Agens.ElasticVertex].schema,
	val schemaV = StructType( Array(
		StructField("timestamp", TimestampType, false),
		StructField("datasource", StringType, false),
		StructField("id", StringType, false),
		StructField("label", StringType, false),
		StructField("properties", new ArrayType(schemaP, true), false)
	))
	// ** Equal to ==> Encoders.product[Agens.ElasticEdge].schema,
	val schemaE = schemaV.
			add(StructField("src", StringType, false)).
			add(StructField("dst", StringType, false))

	// **ref https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/
	def withConvStrValue(colName: String, convType:DataType)(df: DataFrame): DataFrame = {
		df.withColumn( colName+"$", col(colName).cast(convType) )
				.drop( col(colName) )
	}

	def main(args: Array[String]): Unit = {

		// enableHiveSupport() ==> equal to "spark.sql.catalogImplementation=hive"
		val agens: Agens = new Agens()
		val datasource = "modern"

		LOG.info(s"\n===========================================================")
		// TEST: AgensElastic & AgensMeta
		LOG.info(agens.meta.datasource(datasource))
		LOG.info(agens.meta.datasource(datasource).label("person"))
		LOG.info(agens.meta.datasource(datasource).label("person").property("age"))

		LOG.info(s"\n===========================================================")

		val softwareSchema = agens.meta.datasource(datasource).label("software").schema
		LOG.info("softwareSchema => "+softwareSchema)

		LOG.info(s"\n===========================================================")

		val vlName = "person"
		val personDf = agens.readVertex(datasource, vlName)

		personDf.printSchema()
		personDf.show(false)

		val elName = "knows"
		val knowsDf = agens.readEdge(datasource, elName)

		knowsDf.printSchema()
		knowsDf.show(false)

		LOG.info(s"\n===========================================================")

		val helloUdf = functions.udf(AgensHelper.hello)
		agens.spark.udf.register("helloUdf", helloUdf)

		agens.spark.sql("select helloUdf('TEST') as msg").show(false)
		LOG.info(s"\n===========================================================")
/*
beeline> select helloUdf('TEST') as msg;
Error: org.apache.spark.sql.AnalysisException: Undefined function: 'helloUdf'
==> Spark 와 hive 서비스는 별개임.
start-thriftserver.sh 기동시 hive.aux.jars.path 로 jar 을 통해 UDF 를 register 해야 함
 */

		import agens.spark.implicits._		// for using toDS(), toDF()

		val json_str = """
						|{
						|  "a": {
						|     "b": 1,
						|     "c": 2
						|  }
						|}""".stripMargin

		val rdd = agens.spark.sparkContext.parallelize(json_str)
		val df = agens.spark.read.json(Seq(json_str).toDS)		// Dataset[String]
		df.createOrReplaceTempView("json_table")

		val df_res = agens.spark.sql("select a.* from json_table")
		df_res.printSchema()
		df_res.show()

		LOG.info(s"\n===========================================================")

		val sampleSchema = StructType(Array(
			StructField("idVal", IntegerType, nullable = false),
			StructField("intStr", StringType, nullable = true),
			StructField("fltStr", StringType, nullable = true)
		))

		val sample2 = agens.spark.sparkContext.parallelize(Seq(
			Row(1, "100", "100.1"),
			Row(2, "200b", "200.2"),
			Row(3, null, "300.3"),
			Row(4, "400.4", "400.4"),
			Row(5, "500", "500.5")
		))
		val df2 = agens.spark.createDataFrame( sample2, sampleSchema )

		df2.transform( withConvStrValue("intStr",IntegerType) ).show
		// df2.transform( withConvStrValue("fltVal","fltStr",FloatType) ).show

/*
		// https://stackoverflow.com/a/46751214
		val df = agens.spark.sqlContext.esDF(elastic.conf.vertexIndex, elastic.conf.es)
		// Spark.sql with DataFrame made from ES
		val aggDF = df.groupBy("datasource").agg(count("datasource").as("doc_cnt"))

		aggDF.printSchema()
//		root
//		 |-- datasource: string (nullable = true)
//		 |-- doc_cnt: long (nullable = false)
		aggDF.show(10)
//		+----------+-------+
//		|datasource|doc_cnt|
//		+----------+-------+
//		| northwind|   1050|
//		|    modern|      6|
//		| airroutes|   3658|
//		+----------+-------+
 */

	}

}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.Agens \
	target/agens-spark-connector-1.0-dev.jar
*/

class Agens(sc: SparkContext = null) extends Serializable {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "agens-spark-scala"

	// SKIP: enableHiveSupport()
	val spark: SparkSession = if( sc != null ) {
		SparkSession.builder().config(sc.getConf).appName(JOB_NAME).getOrCreate()
	} else{
		SparkSession.builder().appName(JOB_NAME).getOrCreate()
	}
	spark.sparkContext.setLogLevel("error")

	val conf: AgensConfig = if( sc != null ) AgensConfig(sc.getConf) else AgensConfig.default()

	val meta: AgensMeta = AgensMeta(conf)
	import meta._

	///////////////////////////////////////

	def count(datasource:String):(Long, Long) = {
		assert(meta.datasources.contains(datasource), "wrong datasource")
		(this.vertices(datasource).count(), this.edges(datasource).count())
	}

//	def graphFrame(datasource: String):GraphFrame = {
//		assert(meta.datasources.contains(datasource), "wrong datasource")
//		GraphFrame(this.vertices(datasource), this.edges(datasource))
//	}

	///////////////////////////////////////

	// ** TRY: curring function ==> fail for overloaded
	// val elements = AgensHelper.elements(spark,conf.es) _

	// with datasource
	private def elements(index: String, schema: StructType, datasource: String): DataFrame = {
		val query: String = s"""{ "query": { "bool": {
			   |  "must": { "term": { "datasource": "${datasource}" } }
			   |}}}""".stripMargin.replaceAll("\n", " ")
		LOG.info(s"load Vertex Dataframe from '${datasource}'")
		spark.read.format("es").options(conf.es)
				.option("es.query", query)
				.schema(schema)
				.load(index)
	}

	def vertices(datasource: String): DataFrame = {
		LOG.info(s"load Vertex Dataframe from '${datasource}'")
		elements(conf.vertexIndex, schemaV, datasource)
	}
	def edges(datasource: String): DataFrame = {
		LOG.info(s"load Edge Dataframe from '${datasource}'")
		elements(conf.edgeIndex, schemaE, datasource)
	}

	// with datasource, labels
	private def elements(index: String, schema: StructType, datasource: String, label: String): DataFrame = {
		assert(datasource != null && label != null)
		val query: String = s"""{ "query": { "bool": {
			   |  "must": { "term": { "datasource": "${datasource}" } },
			   |  "must": { "term": { "label": "${label}" } }
			   |}}}""".stripMargin.replaceAll("\n", " ")
		spark.read.format("es").options(conf.es)
				.option("es.query", query)
				.schema(schema)
				.load(index)
	}
	def vertices(datasource: String, label: String): DataFrame = {
		LOG.info(s"load Vertex Dataframe from '${datasource}.${label}")
		elements(conf.vertexIndex, schemaV,	datasource, label)
	}
	def edges(datasource: String, label: String): DataFrame = {
		LOG.info(s"load Edge Dataframe from '${datasource}.${label}'")
		elements(conf.edgeIndex, schemaE, datasource, label)
	}

	///////////////////////////////////////
	val propertyIdentityChar = "$"

	def readVertex(datasource:String, label:String): DataFrame = {
		explodeVertex(vertices(datasource, label), datasource, label)
	}
	private def explodeVertex(df: DataFrame, datasource:String, label:String): DataFrame = {
		val metaLabel = meta.datasource(datasource).vlabel(label)
		require(metaLabel != null)

		// STEP0: Base Dataframe
		var baseDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id")
		)
		// STEP1: explode nested array field about properties
		val tmpDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			explode(col("properties")).as("property")
		)
		// STEP2: convert property to column
		metaLabel.properties.values.foreach { p:meta.MetaProperty =>
			val df = tmpDf.filter(col("property.key") === p.name)
					.withColumn("tmp$", col("property.value"))
			val convDf = df.withColumn( p.name+propertyIdentityChar, col("tmp$").cast(p.dataType()) )
					.drop( col("property") ).drop(col("tmp$"))
			baseDf = baseDf.join(convDf, Seq("timestamp","datasource","label","id"),"left")
		}
		baseDf
	}

	def readEdge(datasource:String, label:String): DataFrame = {
		explodeEdge(edges(datasource, label), datasource, label)
	}
	private def explodeEdge(df: DataFrame, datasource:String, label:String): DataFrame = {
		val metaLabel = meta.datasource(datasource).elabel(label)
		require(metaLabel != null)

		// STEP0: Base Dataframe
		var baseDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			col("src"),
			col("dst")
		)
		// STEP1: explode nested array field about properties
		val tmpDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			col("src"),
			col("dst"),
			explode(col("properties")).as("property")
		)
		// STEP2: convert property to column
		metaLabel.properties.values.foreach { p:meta.MetaProperty =>
			val df = tmpDf.filter(col("property.key") === p.name)
					.withColumn("tmp$", col("property.value"))
			val convDf = df.withColumn( p.name+propertyIdentityChar, col("tmp$").cast(p.dataType()) )
					.drop( col("property") ).drop(col("tmp$"))
			baseDf = baseDf.join(convDf, Seq("timestamp","datasource","label","id","src","dst"),"left")
		}
		baseDf
	}

}
