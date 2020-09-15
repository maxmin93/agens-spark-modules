package net.bitnine.agens.spark

case class AgensConf(
		var host: String = "localhost",
		var port: String = "9200",				// Int = 9200,
		var user: String = "",					// default: elastic
		var password: String = "",				// default: changeme
		var vertexIndex: String = "agensvertex",
		var edgeIndex: String = "agensedge"
) {
	def es = Map[String,String](
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
