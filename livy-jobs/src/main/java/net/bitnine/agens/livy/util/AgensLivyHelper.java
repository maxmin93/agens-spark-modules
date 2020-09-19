package net.bitnine.agens.livy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public final class AgensLivyHelper {

    // for add jar to livy-session
    public static String connectorJarPath = "hdfs://minmac:9000/user/agens/lib/agens-spark-connector-1.0-dev.jar";
    public static String livyjobsJarPath = "hdfs://minmac:9000/user/agens/lib/agens-livy-jobs-1.0-dev.jar";
    public static String essparkJarPath = "hdfs://minmac:9000/user/agens/jars/elasticsearch-spark-20_2.11-7.7.1.jar";

    public static URI convertURI(String param){
        try{
            return new URI(param);
        }
        catch (URISyntaxException ex){
            return null;
        }
    }

    public static boolean existsHdfsFile(String hdfsUrl){
        try{
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path(hdfsUrl);
            return fileSystem.exists(path);
        }
        catch (Exception ex){
            return false;
        }
    }

}
