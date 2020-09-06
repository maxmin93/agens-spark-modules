package net.bitnine.agens.spark;

import net.bitnine.agens.spark.livy.PiJob;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;


public class AgensJava implements Serializable {

    private static final Logger LOG = Logger.getLogger(AgensJava.class);

    public static final String JOB_NAME = "agens-spark-java";
    private final SparkSession spark;
    private final JavaSparkContext jsc;

    public AgensJava(SparkContext sc){
        if( sc != null ) this.spark = SparkSession.builder().config(sc.getConf()).getOrCreate();
        else this.spark = SparkSession.builder().appName(this.JOB_NAME).getOrCreate();
        this.jsc = new JavaSparkContext(spark.sparkContext());
        this.LOG.info("** created AgensJavaSparkContext");
    }
    public AgensJava(){
        this(null);
    }

    public JavaSparkContext getJsc(){ return this.jsc; }

    public static void main( String[] args ) {
        System.out.println( "net.bitnine.agens.spark" );
    }
}
