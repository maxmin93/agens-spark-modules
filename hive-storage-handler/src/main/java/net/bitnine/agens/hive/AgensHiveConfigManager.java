package net.bitnine.agens.hive;

import org.apache.hadoop.conf.Configuration;

import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

// **참고 : hive-jdbc-storage-handler
public class AgensHiveConfigManager {

    public static final String CONFIG_PREFIX = "agens.query";
    private static final EnumSet<net.bitnine.agens.livy.AgensHiveConfig> DEFAULT_REQUIRED_PROPERTIES = EnumSet.of(
                    net.bitnine.agens.livy.AgensHiveConfig.LIVY_URL,
                    net.bitnine.agens.livy.AgensHiveConfig.DATASOURCE,
                    net.bitnine.agens.livy.AgensHiveConfig.NAME,
                    net.bitnine.agens.livy.AgensHiveConfig.QUERY
    );

    private AgensHiveConfigManager() {
    }

    public static void copyConfigurationToJob(Properties props, Map<String, String> jobProps) {
        checkRequiredPropertiesAreDefined(props);
        for (Entry<Object, Object> entry : props.entrySet()) {
            jobProps.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
    }

    public static Configuration convertPropertiesToConfiguration(Properties props) {
        checkRequiredPropertiesAreDefined(props);
        Configuration conf = new Configuration();

        for (Entry<Object, Object> entry : props.entrySet()) {
            conf.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        return conf;
    }

    private static void checkRequiredPropertiesAreDefined(Properties props) {
        for (net.bitnine.agens.livy.AgensHiveConfig configKey : DEFAULT_REQUIRED_PROPERTIES) {
            String propertyKey = configKey.fullName();
            if ((props == null) || (!props.containsKey(propertyKey)) || (isEmptyString(props.getProperty(propertyKey)))) {
                throw new IllegalArgumentException("Property " + propertyKey + " is required.");
            }
        }
    }


    public static String getConfigValue(net.bitnine.agens.livy.AgensHiveConfig key, Configuration config) {
        return config.get(key.fullName());
    }


    public static String getQueryToExecute(Configuration config) {
        String query = config.get(AgensHiveConfig.QUERY.fullName());

        return query;
    }


    private static boolean isEmptyString(String value) {
        return ((value == null) || (value.trim().isEmpty()));
    }
}
