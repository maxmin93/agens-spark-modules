package net.bitnine.agens.livy;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.livy.*;
import org.apache.spark.launcher.SparkLauncher;

public class ExecuteCypher {

    public static URI convertURI(String param){
        try{
            return new URI(param);
        }
        catch (URISyntaxException ex){
            return null;
        }
    }

    public static String runCypher(Map<String,String> parameters) throws AgensLivyJobException {

        URI livyUri = convertURI(parameters.get(AgensHiveConfig.LIVY_URL.fullName()));
        if( livyUri == null )
            throw new AgensLivyJobException("Wrong livy URI: "+parameters.get(AgensHiveConfig.LIVY_URL.fullName()));

        LivyClient client;
        try {
            client = new LivyClientBuilder()
                    .setURI(livyUri)
                    .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                    .build();
        }
        catch (IOException ex){
            throw new AgensLivyJobException("Fail: livyClient connect", ex.getCause());
        }

        String schemaJson = null;
        try {
            schemaJson = client.submit(new net.bitnine.agens.livy.jobs.AvroWriteJob()).get();
        } catch (Exception ex){
            throw new AgensLivyJobException("Fail: livyClient.submit", ex.getCause());
        } finally {
            client.stop(true);
        }

        return schemaJson;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: ExecuteCypher <livyUrl> <datasource> <name> <cypher>");
            System.exit(-1);
        }

        // options: livyUri, datasource, name, query
        Map<String,String> parameters = new HashMap<>();
        parameters.put(AgensHiveConfig.LIVY_URL.fullName(), args[0]);
        parameters.put(AgensHiveConfig.DATASOURCE.fullName(), args[1]);
        parameters.put(AgensHiveConfig.NAME.fullName(), args[2]);
        parameters.put(AgensHiveConfig.QUERY.fullName(), args[3]);

        try{
            String schemaJson = runCypher(parameters);
            // for DEBUG
            System.out.println("schema ==>\n" + schemaJson);
        }
        catch (AgensLivyJobException ex){
            System.err.printf("Error: LivyClient could not be build for URI[%s]\n", args[0]);
            System.exit(-1);
        }
    }
}

/*
java -cp target/agens-livy-jobs-1.0-dev.jar net.bitnine.agens.livy.PiApp http://minmac:8998 2
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */