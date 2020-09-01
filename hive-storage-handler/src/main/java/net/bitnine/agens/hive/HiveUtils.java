package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.HiveFieldExtractor;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.*;
import org.elasticsearch.hadoop.util.unit.Booleans;

import java.util.*;
import java.util.Map.Entry;


abstract class HiveUtils {

    // Date type available since Hive 0.12
    static final boolean DATE_WRITABLE_AVAILABLE = ObjectUtils.isClassPresent(HiveConstants.DATE_WRITABLE,
            TimestampWritable.class.getClassLoader());

    static StandardStructObjectInspector structObjectInspector(Properties tableProperties) {
        // extract column info - don't use Hive constants as they were renamed in 0.9 breaking compatibility
        // the column names are saved as the given inspector to #serialize doesn't preserves them (maybe because it's an external table)
        // use the class since StructType requires it ...
        List<String> columnNames = StringUtils.tokenize(tableProperties.getProperty(HiveConstants.COLUMNS), ",");
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(tableProperties.getProperty(HiveConstants.COLUMNS_TYPES));

        // create a standard writable Object Inspector - used later on by serialization/deserialization
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();

        for (TypeInfo typeInfo : colTypes) {
            inspectors.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo));
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    static StructTypeInfo typeInfo(StructObjectInspector inspector) {
        return (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(inspector);
    }

    /**
     * Renders the full collection of field names needed from ES by combining the names of
     * the hive table fields with the user provided name mappings.
     * @param settings Settings to pull hive column names and user name mappings from.
     * @return A collection of ES field names
     */
    static Collection<String> columnToAlias(Settings settings) {
        FieldAlias fa = alias(settings);
        List<String> columnNames = StringUtils.tokenize(settings.getProperty(HiveConstants.COLUMNS), ",");

        // eliminate virtual columns
        // we can't use virtual columns since some distro don't have this field...
        for (String vc : HiveConstants.VIRTUAL_COLUMNS) {
            columnNames.remove(vc);
        }

        for (int i = 0; i < columnNames.size(); i++) {
            String original = columnNames.get(i);
            String alias = fa.toES(original);
            if (alias != null) {
                columnNames.set(i, alias);
            }
        }
        return columnNames;
    }

    /**
     * Reads the current aliases, and then the set of hive column names. Remaps the raw hive column names (_col1, _col2)
     * to the names used in the hive table, or, if the mappings exist, the names in the mappings instead.
     * @param settings Settings to pull user name mappings and hive column names from
     * @return FieldAlias mapping object to go from hive column name to ES field name
     */
    static FieldAlias alias(Settings settings) {
        Map<String, String> aliasMap = SettingsUtils.aliases(settings.getProperty(HiveConstants.MAPPING_NAMES), true);

        // add default aliases for serialization (_colX -> mapping name)
        Map<String, String> columnMap = columnMap(settings);

        for (Entry<String, String> entry : columnMap.entrySet()) {
            String columnName = entry.getKey();
            String columnIndex = entry.getValue();

            if (!aliasMap.isEmpty()) {
                String alias = aliasMap.get(columnName);
                if (alias != null) {
                    columnName = alias;
                }
            }

            aliasMap.put(columnIndex, columnName);
        }

        return new FieldAlias(aliasMap, true);
    }

    /**
     * Selects an appropriate field from the given Hive table schema to insert JSON data into if the feature is enabled
     * @param settings Settings to read schema information from
     * @return A FieldAlias object that projects the json source field into the select destination field
     */
    static String discoverJsonFieldName(Settings settings, FieldAlias alias) {
        Set<String> virtualColumnsToBeRemoved = new HashSet<String>(HiveConstants.VIRTUAL_COLUMNS.length);
        Collections.addAll(virtualColumnsToBeRemoved, HiveConstants.VIRTUAL_COLUMNS);

        List<String> columnNames = StringUtils.tokenize(settings.getProperty(HiveConstants.COLUMNS), ",");
        Iterator<String> nameIter = columnNames.iterator();

        List<String> columnTypes = StringUtils.tokenize(settings.getProperty(HiveConstants.COLUMNS_TYPES), ":");
        Iterator<String> typeIter = columnTypes.iterator();

        String candidateField = null;

        while(nameIter.hasNext() && candidateField == null) {
            String columnName = nameIter.next();
            String type = typeIter.next();

            if ("string".equalsIgnoreCase(type) && !virtualColumnsToBeRemoved.contains(columnName)) {
                candidateField = columnName;
            }
        }

        Assert.hasText(candidateField, "Could not identify a field to insert JSON data into " +
                "from the given fields : {" + columnNames + "} of types {" + columnTypes + "}");

        // If the candidate field is aliased to something else, find the alias name and use that for the field name:
        candidateField = alias.toES(candidateField);

        return candidateField;
    }

    /**
     * @param settings The settings to extract the list of hive columns from
     * @return A map of column names to "_colX" where "X" is the place in the struct
     */
    static Map<String, String> columnMap(Settings settings) {
        return columnMap(settings.getProperty(HiveConstants.COLUMNS));
    }

    /**
     * @param columnString The comma separated list of hive column names
     * @return A map of column names to "_colX" where "X" is the place in the struct
     */
    private static Map<String, String> columnMap(String columnString) {
        // add default aliases for serialization (mapping name -> _colX)
        List<String> columnNames = StringUtils.tokenize(columnString, ",");
        if (columnNames.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> columns = new LinkedHashMap<String, String>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.put(columnNames.get(i), HiveConstants.UNNAMED_COLUMN_PREFIX + i);
        }
        return columns;
    }

    static void init(Settings settings, Log log) {
        InitializationUtils.checkIdForOperation(settings);
        InitializationUtils.setFieldExtractorIfNotSet(settings, HiveFieldExtractor.class, log);
        InitializationUtils.discoverClusterInfo(settings, log);
    }

    static void fixHive13InvalidComments(Settings settings, Properties tbl) {
        if (Booleans.parseBoolean(settings.getProperty("es.hive.disable.columns.comments.fix"))) {
            return;
        }

        settings.setProperty(HiveConstants.COLUMN_COMMENTS, "");
        tbl.setProperty(HiveConstants.COLUMN_COMMENTS, "");
    }
}
