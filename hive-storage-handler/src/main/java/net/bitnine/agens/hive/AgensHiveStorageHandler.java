package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class AgensHiveStorageHandler extends DefaultStorageHandler {

    public static final Log LOG = LogFactory.getLog(AgensHiveStorageHandler.class);

    public static void main( String[] args )
    {
        System.out.println( "net.bitnine.agens.hive" );
    }

    private Configuration conf;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return SequenceFileInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return SequenceFileOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return LazySimpleSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        // no hook by default
        return new AgensHiveMetaHook();
    }

    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        // do nothing by default
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
                                             Map<String, String> jobProperties) {
        // do nothing by default
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        //do nothing by default
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        //do nothing by default
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    @Override
    public String toString() {
        return this.getClass().getName();
    }

}

/*
add jar hdfs://minmac:9000/user/agens/lib/agens-hive-storage-handler-1.0-dev.jar;

list jars;

CREATE EXTERNAL TABLE agens_test1(
  breed STRING,
  sex STRING
) STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
  'agens.conf.livy'='http://minmac:8998',
  'agens.conf.jar'='agens-hive-storage-handler-1.0-dev.jar',
  'agens.graph.datasource'='modern',
  'agens.graph.query'='match (a)-[:KNOWS]-(b) return a, b',
);

insert into agens_test1 values('aaa','bbb');
==>
Diagnostic Messages for this Task:
Error: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: Hive Runtime Error while processing row {"tmp_values_col1":"aaa","tmp_values_col2":"bbb"}
Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: org.apache.hadoop.hive.ql.metadata.HiveException: org.apache.hadoop.fs.FileAlreadyExistsException: /user/hive/warehouse/agens_test1 already exists as a directory

drop table agens_test1;
 */