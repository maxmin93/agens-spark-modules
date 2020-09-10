package net.bitnine.agens.hive.livy

import net.bitnine.agens.spark.avro.SchemaConverters
import org.apache.avro.SchemaBuilder
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AvroWriteJob extends Job[java.lang.String] {

	// reference
	// https://sparkbyexamples.com/spark/read-write-avro-file-spark-dataframe/

	override def call(jc: JobContext): java.lang.String = {

		val spark: SparkSession = jc.sparkSession()
		import spark.sqlContext.implicits._

		val columns = Seq("firstname", "middlename", "lastname", "dob_year", "dob_month", "gender", "salary")
		val data = Seq(("James ","","Smith",2018,1,"M",3000),
			("Michael ","Rose","",2010,3,"M",4000),
			("Robert ","","Williams",2010,3,"M",4000),
			("Maria ","Anne","Jones",2005,5,"F",4000),
			("Jen","Mary","Brown",2010,7,"",-1)
		)

		val df = data.toDF(columns:_*)
		df.write.mode(SaveMode.Overwrite)
				//.partitionBy("dob_year")
				.format("avro")
				.save("/user/agens/temp/person.avro")

		val dataSchema = df.schema
		val options: Map[String, String] = Map("recordName"->"avro_person", "recordNamespace"->"net.bitnine.agens.hive")
		val recordName = options.getOrElse("recordName", "topLevelRecord")
		val recordNamespace = options.getOrElse("recordNamespace", "")
		val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
		val outputAvroSchema = SchemaConverters.convertStructToAvro(dataSchema, build, recordNamespace)
		System.err.println(outputAvroSchema.toString(true))

		// outputAvroSchema.getFields.asScala.map(_.schema().toString).mkString("[", "],\n[", "]")
		// outputAvroSchema.getFields
		outputAvroSchema.toString
	}

}

/*

// Creating the string from an existing dataframe
val schema = df.schema
val jsonString = schema.json

// create a schema from json
import org.apache.spark.sql.types.{DataType, StructType}
val newSchema = DataType.fromJson(jsonString).asInstanceOf[StructType]

----------------------------

val schemaAvro = new Schema.Parser()
      .parse(new File("src/main/resources/person.avsc"))

val df = spark.read
              .format("avro")
              .option("avroSchema", schemaAvro.toString)
              .load("person.avro")

 */

/*

add jar hdfs://minmac:9000/user/agens/lib/agens-hive-storage-handler-1.0-dev.jar;

list jars;

CREATE external TABLE agens_test1 (
firstname string,
middlename string,
lastname string,
dob_year int,
dob_month int,
gender string,
salary int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/agens/temp/person.avro'
TBLPROPERTIES ('COLUMN_STATS_ACCURATE'='false');

create external table agens_test2 (
firstname string,
middlename string,
lastname string,
dob_year int,
dob_month int,
gender string,
salary int
) STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'agens.graph.livy'='http://minmac:8998',
'agens.graph.datasource'='modern',
'agens.graph.query.name'='cypher_test',
'agens.graph.query.cypher'='match (a)-[:KNOWS]-(b) return a, b'
);

-- Columns 들은 정상적으로 파싱되어 들어 왔는데, Spark 작업중 자꾸 에러남
create external table agens_test3
STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'agens.graph.livy'='http://minmac:8998',
'agens.graph.datasource'='modern',
'agens.graph.name'='cypher_test',
'agens.query.cypher'='match (a)-[:KNOWS]-(b) return a, b',
'avro.schema.literal'='{"type":"record","name":"avro_person","namespace":"net.bitnine.agens.hive","fields":[{"name":"firstname","type":["string","null"]},{"name":"middlename","type":["string","null"]},{"name":"lastname","type":["string","null"]},{"name":"dob_year","type":"int"},{"name":"dob_month","type":"int"},{"name":"gender","type":["string","null"]},{"name":"salary","type":"int"}]}'
);

-- **NOTE: 컬럼 설정을 안할 경우 MetaHook 들어가기도 전에 DDLTask 단계에서 실패
-- ==> 1) 컬럼을 정의하거나, 2) avro.schema 의 fields 를 정의해야 함

create external table agens_test4
STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'avro.schema.url'='/user/agens/default.avsc',
'agens.graph.livy'='http://minmac:8998',
'agens.graph.datasource'='modern',
'agens.graph.name'='cypher_test',
'agens.graph.query'='match (a)-[:KNOWS]-(b) return a, b'
);

-- dummy column 을 넣을까? 반드시 들어갈 내용이 있나?
create external table agens_test5 (id string)
STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'agens.graph.livy'='http://minmac:8998',
'agens.graph.datasource'='modern',
'agens.graph.query'='match (a)-[:KNOWS]-(b) return a, b'
);

Properties ==>
//////////////////////////////////////////////////////

columns=,
columns.types=,
columns.comments=,

file.inputformat=org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat,
file.outputformat=org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat,
serialization.lib=org.apache.hadoop.hive.serde2.avro.AvroSerDe,
storage_handler=net.bitnine.agens.hive.AgensHiveStorageHandler,

name=default.agens_test4

location=hdfs://minmac:9000/user/agens/temp/person.avro,
avro.schema.literal={"type":"record","name":"avro_person","namespace":"net.bitnine.agens.hive","fields":[{"name":"firstname","type":["string","null"]},{"name":"middlename","type":["string","null"]},{"name":"lastname","type":["string","null"]},{"name":"dob_year","type":"int"},{"name":"dob_month","type":"int"},{"name":"gender","type":["string","null"]},{"name":"salary","type":"int"}]},

agens.graph.livy=http://minmac:8998,
agens.graph.datasource=modern,
agens.graph.query=match (a)-[:KNOWS]-(b) return a, b,

//////////////////////////////////////////////////////

create external table avro_person (
firstname string,
middlename string,
lastname string,
dob_year int,
dob_month int,
gender string,
salary int
) STORED AS AVRO
LOCATION '/user/agens/temp/person.avro';

hive> select * from avro_person;
OK
James 		Smith	2018	1	M	3000
Michael 	Rose		2010	3	M	4000
Robert 		Williams	2010	3	M	4000
Maria 	Anne	Jones	2005	5	F	4000
Jen	Mary	Brown	2010	7		-1
Time taken: 1.608 seconds, Fetched: 5 row(s)

** copy hive_table to hdfs directory using avro
  ==> Error, return code 1 from org.apache.hadoop.hive.ql.exec.MoveTask
INSERT OVERWRITE LOCAL DIRECTORY '/user/hive/warehouse/person_copy.avro'
STORED AS AVRO SELECT * FROM avro_person;

 */