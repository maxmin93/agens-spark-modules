package net.bitnine.agens.hive;


import com.google.common.collect.ImmutableList;

public class AgensHiveConstants {

    public static final String UNIQUE_JOB_KEY = "mapred.ag.unique.job.id";
    public static final String DEFAULT_AGENS_DATASET_KEY = "ag.dataset";
    public static final String DEFAULT_AGENS_TABLE_KEY = "ag.table";

    public static final String PREDICATE_PUSHDOWN_COLUMNS = "ppd.columns";
    public static final String HIVE_PROJECTION_COLUMNS = "hive.io.file.readcolumn.names";
    public static final String AGENS_PROJECTION_COLUMNS = "mapred.ag.input.selected.fields";
    public static final String AGENS_FILTER_EXPRESSION = "mapred.ag.input.sql.filter";
    public static final String DELIMITER = ",";

    public static final ImmutableList<String> MANDATORY_TABLE_PROPERTIES = ImmutableList.of(
            "agens.graph.datasource", "agens.graph.type", "agens.graph.label"
    );
    public static final ImmutableList<String> PREDICATE_PUSHDOWN_ALLOWED_TYPES = ImmutableList.of(
            "int", "bigint", "float", "double", "string", "boolean", "timestamp", "date"
    );
}
