package net.bitnine.agens.livy.scala

import org.apache.livy.{Job, JobContext}

import scala.collection.JavaConverters._

class PiJob(val slices: Int) extends Job[java.lang.Double] {

	val samples = Math.min(100000L * slices, Integer.MAX_VALUE).toInt

	override def call(jc: JobContext): java.lang.Double = {
		// Pi Estimation
		// https://spark.apache.org/examples.html

		val count = jc.sc.parallelize(1 to samples).filter { _ =>
			val x = math.random
			val y = math.random
			x*x + y*y < 1
		}.count()

		val pi = 4.0 * count / samples
		pi
	}

}