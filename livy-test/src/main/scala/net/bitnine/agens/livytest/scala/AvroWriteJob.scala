package net.bitnine.agens.livytest.scala

import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AvroWriteJob extends Job[java.lang.String] {

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