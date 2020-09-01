package net.bitnine.agens.hive;


interface HiveConstants {

    String COLUMNS = "columns";
    String COLUMNS_TYPES = "columns.types";
    String UNNAMED_COLUMN_PREFIX="_col";

    String DECIMAL_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable";
    String DATE_WRITABLE = "org.apache.hadoop.hive.serde2.io.DateWritable";
    String VARCHAR_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveVarcharWritable";
    String CHAR_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveCharWritable";
    String TABLE_LOCATION = "location";

    String MAPPING_NAMES = "es.mapping.names";
    String COLUMN_COMMENTS = "columns.comments";

    String INPUT_TBL_PROPERTIES = "es.internal.hive.input.tbl.properties";
    String OUTPUT_TBL_PROPERTIES = "es.internal.hive.output.tbl.properties";
    String[] VIRTUAL_COLUMNS = new String[] { "INPUT__FILE__NAME", "BLOCK__OFFSET__INSIDE__FILE",
            "ROW__OFFSET__INSIDE__BLOCK", "RAW__DATA__SIZE", "GROUPING__ID" };
}
