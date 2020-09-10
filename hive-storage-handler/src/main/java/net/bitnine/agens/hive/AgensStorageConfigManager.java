package net.bitnine.agens.hive;

import org.apache.hadoop.conf.Configuration;

import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

// **참고 : hive-jdbc-storage-handler
public class AgensStorageConfigManager {

    public static final String CONFIG_PREFIX = "agens.graph";
    private static final EnumSet<AgensStorageConfig> DEFAULT_REQUIRED_PROPERTIES = EnumSet.of(
                    AgensStorageConfig.LIVY_URL,
                    AgensStorageConfig.DATASOURCE,
                    AgensStorageConfig.NAME,
                    AgensStorageConfig.QUERY
    );

    private AgensStorageConfigManager() {
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
        for (AgensStorageConfig configKey : DEFAULT_REQUIRED_PROPERTIES) {
            String propertyKey = configKey.getPropertyName();
            if ((props == null) || (!props.containsKey(propertyKey)) || (isEmptyString(props.getProperty(propertyKey)))) {
                throw new IllegalArgumentException("Property " + propertyKey + " is required.");
            }
        }

//        DatabaseType dbType = DatabaseType.valueOf(props.getProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()));
//        CustomConfigManager configManager = CustomConfigManagerFactory.getCustomConfigManagerFor(dbType);
//        configManager.checkRequiredProperties(props);
    }


    public static String getConfigValue(AgensStorageConfig key, Configuration config) {
        return config.get(key.getPropertyName());
    }


    public static String getQueryToExecute(Configuration config) {
        String query = config.get(AgensStorageConfig.QUERY.getPropertyName());

        return query;
    }


    private static boolean isEmptyString(String value) {
        return ((value == null) || (value.trim().isEmpty()));
    }
}
