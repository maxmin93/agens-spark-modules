package net.bitnine.agens.hive.livy

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.CAPSSession._
import net.bitnine.agens.cypher.examples.SocialNetworkData

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.livy.{Job, JobContext}

// import scala.language.implicitConversions

class CypherJob extends Job[java.lang.Long] {

	override def call(jc: JobContext): java.lang.Long = {
		val spark = jc.sparkSession[SparkSession]()

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

		// 3) Query graph with Cypher
		val results = socialNetwork.cypher(
			"""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
			   |RETURN a.name, b.name, r.since
			   |ORDER BY a.name""".stripMargin
		)

		// 4) Extract DataFrame representing the query result
		val df: DataFrame = results.records.asDataFrame
		df.show(false)

		java.lang.Long.valueOf(df.count())
	}
}
