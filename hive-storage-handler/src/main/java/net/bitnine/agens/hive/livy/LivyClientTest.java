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
                    .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                    .setConf("livy.rsc.server.connect.timeout","10s")
                    .setConf("agens.livy.host","minmac")
                    .setConf("agens.livy.port","8998")
                    .setConf("agens.es.vertex","agensvertex")
                    .setConf("agens.es.edge","agensedge")
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

//            int samples = Integer.parseInt(args[1]);
//            System.err.printf("Running PiJob with '%d' samples...\n", samples);
//            pi = client.submit(new net.bitnine.agens.hive.livy.PiJob(samples)).get();
//            System.out.println("Pi is roughly: " + pi);

//            Long size = client.submit(new net.bitnine.agens.hive.livy.AvroWriteJob()).get();
//            System.out.printf("Df.size = %d\n", size);

            String json = client.submit(new net.bitnine.agens.hive.livy.AvroWriteJob()).get();
            System.out.println("schema ==>\n"+json);

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
        config.set("fs.default.name", "hdfs://minmac:9000");
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

java -cp target/agens-hive-storage-handler-1.0-dev.jar net.bitnine.agens.hive.livy.LivyClientTest http://minmac:8998 5

 */