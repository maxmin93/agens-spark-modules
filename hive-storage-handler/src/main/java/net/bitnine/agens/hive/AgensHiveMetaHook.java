package net.bitnine.agens.hive;

import com.google.common.base.Strings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.bitnine.agens.hive.livy.LivyClientTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;

import java.util.*;

public class AgensHiveMetaHook implements HiveMetaHook {

    public static final Log LOG = LogFactory.getLog(AgensHiveMetaHook.class);

    /**
     * Performs required validations prior to creating the table
     *
     * @param table Represents hive table object
     * @throws MetaException if table metadata violates the constraints
     */
    @Override
    public void preCreateTable(Table table) throws MetaException {

        // Check all mandatory table properties
        for (String property : AgensHiveConstants.MANDATORY_TABLE_PROPERTIES) {
            if (Strings.isNullOrEmpty(table.getParameters().get(property))) {
                throw new MetaException(property + " table property cannot be empty.");
            }
        }

        if(!Strings.isNullOrEmpty(table.getSd().getLocation())) {
            throw new MetaException("Cannot create table in BigQuery with Location property.");
        }

        // external table using avro
        // location:hdfs://minmac:9000/user/agens/temp/person.avro,
        // inputFormat:org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat,
        // outputFormat:org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat,
        StorageDescriptor sd = table.getSd();

        System.err.printf("preCreateTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("==> "+table.getSd()+"\n");

        // remove columns
        sd.setCols(new ArrayList<FieldSchema>());
        // remove avro default schema (actually this is a dummy)
        table.getParameters().remove("avro.schema.url");

        // set location of avro result after executing cypher
        sd.setLocation("/user/agens/temp/person.avro");
        // set formats for avro input and output
        sd.setInputFormat(AvroContainerInputFormat.class.getCanonicalName());
        sd.setOutputFormat(AvroContainerOutputFormat.class.getCanonicalName());

        String schemaJson = ExecuteCypher.runCypher(table.getParameters());
        table.getParameters().put("avro.schema.literal", schemaJson);
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("rollbackCreateTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("==> "+table.getSd()+"\n");

    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("commitCreateTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("==> "+table.getSd()+"\n");

    }

    @Override
    public void preDropTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("preDropTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("==> "+table.getSd()+"\n");

    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("rollbackDropTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("==> "+table.getSd()+"\n");

    }

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException {
        // Do nothing by default
        System.err.printf("commitDropTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("==> "+table.getSd()+"\n");

    }
}
