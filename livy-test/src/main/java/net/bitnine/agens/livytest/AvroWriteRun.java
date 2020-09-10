package net.bitnine.agens.livytest;

import org.apache.avro.Schema;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.net.URI;
import java.util.List;

public class AvroWriteRun {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: AvroWriteRun <livy url>");
            System.exit(-1);
        }

        // Properties config = new Properties();
        // LivyClient client = new HttpClientFactory().createClient(new URI(livyUrl), config);
        LivyClient client = new LivyClientBuilder()
                .setURI(new URI(args[0]))
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                .setConf("livy.rsc.server.connect.timeout","10s")
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

//             String json = client.submit(new net.bitnine.agens.livytest.scala.AvroWriteJob()).get();
//             System.out.println("schema ==>\n"+json);

//            List<Schema.Field> fields = client.submit(new net.bitnine.agens.livytest.scala.AvroWriteJob()).get();
//            for( Schema.Field field : fields ){
//                System.out.printf("{ name: '%s', type: '%s', doc: '%s' }\n",
//                        field.schema().getName(), field.schema().getType(), field.schema().getDoc()
//                        );
//            }

            Schema schema = client.submit(new net.bitnine.agens.livytest.scala.AvroWriteJob()).get();
            System.out.println("schema ==>\n"+schema.toString(true));

        } finally {
            client.stop(true);
        }
    }
}

/*
java -cp target/agens-livy-test-1.0-dev.jar net.bitnine.agens.livytest.AvroWriteRun http://minmac:8998
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */
