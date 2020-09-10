package net.bitnine.agens.hive;

// **참고 : hive-jdbc-storage-handler
public enum AgensStorageConfig {

    LIVY_URL("livy", true),
    DATASOURCE("datasource", true),
    NAME("name", true),
    QUERY("query", true),
    VERTEX_INDEX("vertex", false),
    EDGE_INDEX("edge", false),
    FETCH_SIZE("fetch.size", false),
    COLUMN_MAPPING("column.mapping", false);

    private String propertyName;
    private boolean required = false;

    AgensStorageConfig(String propertyName, boolean required) {
        this.propertyName = propertyName;
        this.required = required;
    }

    AgensStorageConfig(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return AgensStorageConfigManager.CONFIG_PREFIX + "." + propertyName;
    }

    public boolean isRequired() {
        return required;
    }

}
