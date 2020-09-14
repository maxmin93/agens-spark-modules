package net.bitnine.agens.spark.examples

import net.bitnine.agens.spark.AgensBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ReadAsTableExample extends App {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "ReadAsTableExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	//////////////////////////////////

	val agens = AgensBuilder.default()
	val datasource = "modern"

	val graphModern = agens.graph(datasource)
	println("\n** schema: "+graphModern.schema.toString)

	agens.catalog.store("modern", graphModern)
	println("\n** [start] graphs: "+agens.catalog.graphNames.map(_.graphName).mkString("[","],[","]"))

	val result1 = agens.cypher("FROM GRAPH session.modern MATCH (n) RETURN n")
	result1.show
	val result2 = agens.cypher("FROM GRAPH session.modern MATCH ()-[e]-() RETURN e")
	result2.show

	val result3 = graphModern.cypher("""|MATCH (a:person)-[r:knows]->(b)
										|RETURN a.name$, b.name$, r.weight$
										|ORDER BY a.name$""".stripMargin)
	result3.show

	val result4 = graphModern.cypher("MATCH (a)-[r:knows]->(b) RETURN a, b")
	result4.show

	agens.catalog.dropGraph(datasource)
	println("\n** [end] graphs: "+agens.catalog.graphNames.map(_.graphName).mkString("[","],[","]"))
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.examples.ReadAsTableExample \
	target/agens-spark-connector-1.0-dev.jar

*/