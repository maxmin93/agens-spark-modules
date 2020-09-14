package net.bitnine.agens.spark

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.spark.Agens.{schemaE, schemaV}
import net.bitnine.agens.cypher.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import net.bitnine.agens.spark.AgensHelper.{explodeEdge, explodeVertex, wrappedEdgeTable, wrappedVertexTable}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.okapi.api.graph.{CypherResult, PropertyGraph}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

import scala.language.implicitConversions


object Agens extends Serializable {

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

	def main(args: Array[String]): Unit = {

		// val agens = AgensBuilder(spark).build
		val agens = AgensBuilder.default()
		val datasource = "modern"

		val size = agens.count(datasource)
		val meta = agens.meta.datasource(datasource)

		val graph = agens.graph(datasource)
		println("** schema: "+graph.schema.toString)

		agens.session.catalog.store("modern", graph)

		val result1 = agens.session.cypher("FROM GRAPH session.modern MATCH (n) RETURN n")
		result1.show

		val result2 = agens.session.cypher("FROM GRAPH session.modern MATCH ()-[e]-() RETURN e")
		result2.show

//		val query = "MATCH (a)-[:knows]->[b] RETURN a.name, b.name"
		val result3 = graph.cypher("""|MATCH (a:person)-[r:knows]->(b)
									  |RETURN a.name$, b.name$
									  |ORDER BY a.name$""".stripMargin)
		result3.show

		val result4 = graph.cypher("""|MATCH (a)-[r:knows]->(b)
									  |RETURN a, b""".stripMargin)
		result4.show

		println("\n==========================\n")

		val result5 = graph.cypher("MATCH (a)-[r:knows]->(b) RETURN a, b")
		result5.show

//		val name = "modern_test1"
//		results.saveAsAvro(name)

/*
		LOG.info("\n============================================")

		val graphName = GraphName("sn")
		val graph = socialNetwork.asCaps

		val schema = socialNetwork.schema
		val nodeWrites = schema.labelCombinations.combos.map { combo =>
			val nodeType = combo.toList.sorted.mkString("_")		// multi-label 이라서?
			val tableName = HiveTableName(hiveDatabaseName, graphName, Node, Set(nodeType.toLowerCase))
			val df = graph.canonicalNodeTable(combo)
			df.write.mode("overwrite").saveAsTable(tableName)
			tableName
		}
		val relWrites = schema.relationshipTypes.map { relType =>
			val tableName = HiveTableName(hiveDatabaseName, graphName, Relationship, Set(relType.toLowerCase))
			val df = graph.canonicalRelationshipTable(relType)
			df.write.mode("overwrite").saveAsTable(tableName)
			tableName
		}
		println(s"** nodeWrites: $nodeWrites")
		println(s"** relWrites: $relWrites")

		session.sparkSession.sql(s"show tables in ${hiveDatabaseName}").show
		// **SparkSQL : spark.catalog.listTables("socialnetwork").show

 */

	}

}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.Agens \
	target/agens-spark-connector-1.0-dev.jar
*/

class Agens(spark: SparkSession, conf: AgensConf) extends Serializable {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	implicit private val meta: AgensMeta = AgensMeta(conf)

	implicit private val session: CAPSSession = CAPSSession.create(spark)

	///////////////////////////////////////

	def metaRoot():AgensMeta = this.meta

	def count(datasource:String): Long = {
		assert(meta.datasources.contains(datasource), "wrong datasource")
		this.vertices(datasource).count() + this.edges(datasource).count()
	}

//	def graphFrame(datasource: String):GraphFrame = {
//		assert(meta.datasources.contains(datasource), "wrong datasource")
//		GraphFrame(this.vertices(datasource), this.edges(datasource))
//	}

	def graph(datasource: String): PropertyGraph = {
		val vlabels = meta.datasource(datasource).vertices.keys.toSet
		val vertexTables:List[CAPSEntityTable] = vlabels.map{ label =>
			readVertexAsTable(datasource, label)
		}.toList
		val elabels = meta.datasource(datasource).edges.keys.toSet
		val edgeTables:List[CAPSEntityTable] = elabels.map{ label =>
			readEdgeAsTable(datasource, label)
		}.toList

		session.readFrom(vertexTables ++ edgeTables)
	}

	def cypher(datasource: String, query: String): CypherResult = {
		val graph: PropertyGraph = this.graph(datasource)
		if( graph == null ) return RelationalCypherResult.empty
		graph.cypher(query)
	}

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

	private def vertices(datasource: String): DataFrame = {
		LOG.info(s"load Vertex Dataframe from '${datasource}'")
		elements(conf.vertexIndex, schemaV, datasource)
	}
	private def edges(datasource: String): DataFrame = {
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
	private def vertices(datasource: String, label: String): DataFrame = {
		LOG.info(s"load Vertex Dataframe from '${datasource}.${label}")
		elements(conf.vertexIndex, schemaV,	datasource, label)
	}
	private def edges(datasource: String, label: String): DataFrame = {
		LOG.info(s"load Edge Dataframe from '${datasource}.${label}'")
		elements(conf.edgeIndex, schemaE, datasource, label)
	}

	///////////////////////////////////////

	def readVertexAsDf(datasource:String, label:String): DataFrame = {
		explodeVertex(vertices(datasource, label), datasource, label)
	}
	def readEdgeAsDf(datasource:String, label:String): DataFrame = {
		explodeEdge(edges(datasource, label), datasource, label)
	}

	def readVertexAsTable(datasource:String, label:String): CAPSEntityTable = {
		val df = explodeVertex(vertices(datasource, label), datasource, label)
		// df.show(false)
		wrappedVertexTable(df, label)
	}
	def readEdgeAsTable(datasource:String, label:String): CAPSEntityTable = {
		val df = explodeEdge(edges(datasource, label), datasource, label)
		// df.show(false)
		wrappedEdgeTable(df, label)
	}

	///////////////////////////////////////

}

/*
val spark = SparkSession.builder().master("local").getOrCreate()

// Do all your operations and save it on your Dataframe say (dataFrame)
dataframe.write.avro("/tmp/output")
dataframe.write.format("avro").save(outputPath)
dataframe.write.format("avro").saveAsTable(hivedb.hivetable_avro)
 */