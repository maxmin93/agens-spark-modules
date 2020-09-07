package net.bitnine.agens.hive.livy

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

		df.schema.json
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

CREATE external TABLE agens_test2(
breed STRING,
sex STRING
) STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'agens.conf.livy'='http://minmac:8998',
'agens.conf.jar'='agens-hive-storage-handler-1.0-dev.jar',
'agens.graph.datasource'='modern',
'agens.graph.query'='match (a)-[:KNOWS]-(b) return a, b'
);

CREATE TABLE agens_test1(
  breed STRING,
  sex STRING
) STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
  'agens.conf.livy'='http://minmac:8998',
  'agens.conf.jar'='agens-hive-storage-handler-1.0-dev.jar',
  'agens.graph.datasource'='modern',
  'agens.graph.query'='match (a)-[:KNOWS]-(b) return a, b'
);

CREATE external TABLE agens_test2 (
  `fileld1` string COMMENT '',
  `field2` string COMMENT '',
  `field3` string COMMENT '')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  '/user/agens/temp/person.avro'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false',
  'avro.schema.url'='/data/gaurav/work/hive/old_db/SCHEMA/MyTable.avsc');


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

CREATE external TABLE avro_person_copy
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/agens/temp/person.avro'
TBLPROPERTIES ('avro.schema.literal'='{
"namespace": "net.bitnine.agens.hive",
"name": "doctors",
"type": "record",
"fields": [
{"name":"firstname","type":"string"},
{"name":"middlename","type":["string","null"]},
{"name":"lastname","type":"string","nullable":true},
{"name":"dob_year","type":"int","default":2020},
{"name":"dob_month","type":"int","default":1},
{"name":"gender","type":"string","default":"M"},
{"name":"salary","type":"int"}
]}');


==================================
Detailed Table Information	Table(
==================================
tableName:avro_person,
dbName:default,
owner:bgmin,
createTime:1599481881,
lastAccessTime:0,
retention:0,
sd:StorageDescriptor(
	cols:[
		FieldSchema(name:firstname, type:string, comment:null),
		FieldSchema(name:middlename, type:string, comment:null),
		FieldSchema(name:lastname, type:string, comment:null),
		FieldSchema(name:dob_year, type:int, comment:null),
		FieldSchema(name:dob_month, type:int, comment:null),
		FieldSchema(name:gender, type:string, comment:null),
		FieldSchema(name:salary, type:int, comment:null)
	],
	location:hdfs://minmac:9000/user/agens/temp/person.avro,
	inputFormat:org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat,
	outputFormat:org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat,
	compressed:false,
	numBuckets:-1,
	serdeInfo:SerDeInfo(
		name:null,
		serializationLib:org.apache.hadoop.hive.serde2.avro.AvroSerDe,
		parameters:{serialization.format=1}
	),
	bucketCols:[],
	sortCols:[],
	parameters:{},
	skewedInfo:SkewedInfo(
		skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}
	),
	storedAsSubDirectories:false
),
partitionKeys:[],
parameters:{
	transient_lastDdlTime=1599481881,
	totalSize=1015,
	EXTERNAL=TRUE,
	numFiles=2
},
viewOriginalText:null,
viewExpandedText:null,
tableType:EXTERNAL_TABLE,
rewriteEnabled:false
)

 */