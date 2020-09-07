package net.bitnine.agens.livy;

import java.io.File;
import java.net.URI;

import org.apache.livy.*;
import org.apache.spark.launcher.SparkLauncher;

public class AvroWriteRun {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: AvroWriteRun <livy url>");
            System.exit(-1);
        }

        LivyClient client = new LivyClientBuilder()
                .setURI(new URI(args[0]))
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                .build();

        if (client == null) {
            System.err.printf("Error: LivyClient could not be build for URI[%s]\n", args[0]);
            System.exit(-1);
        }

        try {
            System.out.println("Uploading livy-example jar to the SparkContext...");
            for (String s : System.getProperty("java.class.path").split(File.pathSeparator)) {
                if (new File(s).getName().startsWith("agens-livy")) {
                    client.uploadJar(new File(s)).get();
                    break;
                }
            }

            String json = client.submit(new net.bitnine.agens.livy.scala.AvroWriteJob()).get();

            System.out.println("schema ==>\n"+json);
        } finally {
            client.stop(true);
        }
    }
}

/*
java -cp target/agens-livy-jobs-1.0-dev.jar net.bitnine.agens.livy.PiApp http://minmac:8998 2
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */