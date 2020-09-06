package net.bitnine.agens.livytest;

import java.io.File;
import java.net.URI;

import org.apache.livy.*;
import org.apache.spark.launcher.SparkLauncher;

public class PiApp {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PiJob <livy url> <slices>");
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

            final int slices = Integer.parseInt(args[1]);
            double pi = client.submit(new net.bitnine.agens.livytest.scala.PiJob(slices)).get();

            System.out.println("Pi is roughly " + pi);
        } finally {
            client.stop(true);
        }
    }
}

/*
java -cp target/agens-livy-test-1.0-dev.jar net.bitnine.agens.livytest.PiApp http://minmac:8998 2
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */