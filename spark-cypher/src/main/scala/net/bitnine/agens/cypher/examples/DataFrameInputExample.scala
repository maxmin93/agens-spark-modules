package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.{CAPSNodeTable, CAPSRelationshipTable}

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._


/**
 * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s and
 * running a Cypher query on it.
 */
object DataFrameInputExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "DataFrameInputExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {
		spark.sparkContext.setLogLevel("error")

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Generate some DataFrames that we'd like to interpret as a property graph.
		val nodesDF = SocialNetworkDataFrames.nodes(spark)
		val relsDF = SocialNetworkDataFrames.rels(spark)

		// 3) Generate node- and relationship tables that wrap the DataFrames. The mapping between graph entities and columns
		//    is derived using naming conventions for identifier columns.
		val personTable = CAPSNodeTable(Set("Person"), nodesDF)
		val friendsTable = CAPSRelationshipTable("KNOWS", relsDF)

		// 4) Create property graph from graph scans
		val graph = session.readFrom(personTable, friendsTable)

		// 5) Execute Cypher query and print results
		val result = graph.cypher("MATCH (n:Person) RETURN n.name")

		// 6) Collect results into string by selecting a specific column.
		//    This operation may be very expensive as it materializes results locally.
		// 6a) type safe version, discards values with wrong type
		val safeNames: Set[String] = result.records.collect.flatMap(_ ("n.name").as[String]).toSet
		// 6b) unsafe version, throws an exception when value cannot be cast
		val unsafeNames: Set[String] = result.records.collect.map(_ ("n.name").cast[String]).toSet

		println("\n===============================")
		println(safeNames)

// **results ==>
//		Set(Alice, Bob, Eve)
//		Set(Alice, Bob, Eve)

	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.opencypher.examples.DataFrameInputExample \
	target/agens-spark-cypher-1.0-dev.jar

*/

object SocialNetworkDataFrames {
	def nodes(session: SparkSession): DataFrame = {
		val nodes = List(
			Row(0L, "Alice", 42L),
			Row(1L, "Bob", 23L),
			Row(2L, "Eve", 84L)
		).asJava
		val nodeSchema = StructType(List(
			StructField("id", LongType, false),
			StructField("name", StringType, false),
			StructField("age", LongType, false))
		)
		session.createDataFrame(nodes, nodeSchema)
	}

	def rels(session: SparkSession): DataFrame = {
		val rels = List(
			Row(0L, 0L, 1L, "23/01/1987"),
			Row(1L, 1L, 2L, "12/12/2009")
		).asJava
		val relSchema = StructType(List(
			StructField("id", LongType, false),
			StructField("source", LongType, false),
			StructField("target", LongType, false),
			StructField("since", StringType, false))
		)
		session.createDataFrame(rels, relSchema)
	}
}
