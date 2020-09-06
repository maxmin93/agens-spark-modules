package net.bitnine.agens.hive;

import com.google.common.base.Strings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.bitnine.agens.hive.livy.LivyClientTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

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
/*
        // Check all mandatory table properties
        for (String property : AgensHiveConstants.MANDATORY_TABLE_PROPERTIES) {
            if (Strings.isNullOrEmpty(table.getParameters().get(property))) {
                throw new MetaException(property + " table property cannot be empty.");
            }
        }

        // Check compatibility with BigQuery features
        // TODO: accept DATE column 1 level partitioning
        if (table.getPartitionKeysSize() > 0) {
            throw new MetaException("Creation of Partition table is not supported.");
        }

        if (table.getSd().getBucketColsSize() > 0) {
            throw new MetaException("Creation of bucketed table is not supported");
        }

        if(!Strings.isNullOrEmpty(table.getSd().getLocation())) {
            throw new MetaException("Cannot create table in BigQuery with Location property.");
        }
*/
        System.err.printf("preCreateTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());

        List<String> args = ImmutableList.of("http://minmac:8998", "4");
        LivyClientTest.main(args.toArray(new String[args.size()]));
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("rollbackCreateTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("commitCreateTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());

        List<String> args = ImmutableList.of("http://minmac:8998", "3");
        LivyClientTest.main(args.toArray(new String[args.size()]));
    }

    @Override
    public void preDropTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("preDropTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());

        List<String> args = ImmutableList.of("http://minmac:8998", "3");
        LivyClientTest.main(args.toArray(new String[args.size()]));
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
        // Do nothing by default
        System.err.printf("rollbackDropTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
    }

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException {
        // Do nothing by default
        System.err.printf("commitDropTable(Table): %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());

        List<String> args = ImmutableList.of("http://minmac:8998", "4");
        LivyClientTest.main(args.toArray(new String[args.size()]));
    }
}
