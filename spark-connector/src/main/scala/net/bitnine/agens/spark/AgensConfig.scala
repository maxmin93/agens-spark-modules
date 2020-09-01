package net.bitnine.agens.spark

import org.apache.spark.SparkConf


case class AgensConfig(
		val host: String,
		val port: String,				// Int = 9200,
		val user: String = null,
		val password: String = null,
		val vertexIndex: String = "agensvertex",
		val edgeIndex: String = "agensedge"
) {
	val es = Map(
		"es.nodes"->this.host,
		"es.port"->this.port,
		"es.nodes.wan.only"->"true",
		"es.mapping.id"->"id",
		"es.write.operation"->"upsert",
		"es.index.auto.create"->"true",
		"es.scroll.size"->"10000",
		"es.mapping.date.rich"->"true",				// for timestamp (if don't want convert, false => string)
		"es.spark.dataframe.write.null"->"true",	// write null for fitting StructType
		"es.net.http.auth.user"->this.user,			// for elasticsearch security
		"es.net.http.auth.pass"->this.password		// => curl -u user:password
	)
}

object AgensConfig {

	val prefix = "spark.agens"

	def apply(sparkConf: SparkConf): AgensConfig = {
		val host = sparkConf.getOption(s"$prefix.host").getOrElse("localhost")
		val port = sparkConf.getOption(s"$prefix.port").getOrElse("9200")
				// .map(integer => integer.toInt)
		val vertexIndex = sparkConf.getOption(s"$prefix.vertexIndex").getOrElse("agensvertex")
		val edgeIndex = sparkConf.getOption(s"$prefix.edgeIndex").getOrElse("agensedge")

		val user = sparkConf.getOption(s"$prefix.user").get			// default: elastic
		val password = sparkConf.getOption(s"$prefix.password").get	// default: changeme

		AgensConfig(host, port, user, password, vertexIndex, edgeIndex)
	}

	def default(): AgensConfig = {
		AgensConfig("tonyne.iptime.org", "29200", "elastic", "bitnine", "agensvertex", "agensedge")
	}
}
