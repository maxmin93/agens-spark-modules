package net.bitnine.agens.hive.livy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.net.URI;

public class LivyClientTest {

    public static void main(String[] args)
    {
        // System.out.println( "net.bitnine.agens.hive.livy" );
        if (args.length != 2) {
            System.err.println("Usage: LivyClientTest <livy url> <slices>");
            System.exit(-1);
        }

        LivyClient client;
        double pi = 0.0f;
        try {
            client = new LivyClientBuilder()
                    .setURI(new URI(args[0]))
                    .setConf("es.nodes","minmac")
                    .setConf("es.port","29200")
                    .setConf("es.nodes.wan.only","true")
                    .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                    .build();
        }catch (Exception e){
            return;
        }

        try {
            String jarUri = "hdfs://minmac:9000/user/agens/lib/agens-hive-storage-handler-1.0-dev.jar";
//            if( !existsHdfsFile(jarUri) ){
//                System.err.printf("Error: not found '%s'\n", jarUri);
//                return;
//            }

            System.err.printf("addJar '%s'\n", jarUri);
            client.addJar(new URI(jarUri)).get();       // like as addFile(URI), uploadJar(File)

            int samples = Integer.parseInt(args[1]);
            System.err.printf("Running PiJob with '%d' samples...\n", samples);
            pi = client.submit(new net.bitnine.agens.hive.livy.PiJob(samples)).get();

            System.out.println("Pi is roughly: " + pi);
        }catch (Exception e){
            System.out.println("LivyClient Exception: " + e.getMessage());
        }
        finally {
            client.stop(true);
        }
        // return pi;
    }

    // 왜 안되지? config 탓인가??
    public static boolean existsHdfsFile(String fileName){
        Configuration config = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(config);
            Path path = new Path(fileName);
            return hdfs.exists(path);
        } catch (Exception e){
            return false;
        }
    }
}

/*
java -cp target/agens-hive-storage-handler-1.0-dev.jar net.bitnine.agens.hive.livy.LivyClientTest http://minmac:8998 5


 */