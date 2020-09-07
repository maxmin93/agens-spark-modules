package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
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
        return MapredParquetInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return MapredParquetOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return ParquetHiveSerDe.class;
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

** NOTE: 사용자 변수는 --hiveconf 보다 --hivevar 사용을 권장
select ${hiveconf:hive.server2.thrift.port};
==> 9999

CREATE external TABLE agens_test2(
breed STRING,
sex STRING
) STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'agens.conf.livy'='http://minmac:8998',
'agens.conf.jar'='agens-hive-storage-handler-1.0-dev.jar',
'agens.graph.datasource'='modern',
'agens.graph.query'='match (a)-[:KNOWS]-(b) return a, b'
);

insert into agens_test1 values('aaa','bbb');
==>
Diagnostic Messages for this Task:
Error: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: Hive Runtime Error while processing row {"tmp_values_col1":"aaa","tmp_values_col2":"bbb"}
Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: org.apache.hadoop.hive.ql.metadata.HiveException: org.apache.hadoop.fs.FileAlreadyExistsException: /user/hive/warehouse/agens_test1 already exists as a directory

drop table agens_test1;
 */

/*
** AgensHiveStorageHandler 로 생성한 테이블 (overwrite 가 안됨 )
==========================
Detailed Table Information
==========================
Table(
    tableName:agens_test1,
    dbName:default,
    owner:bgmin,
    createTime:1599440530,
    lastAccessTime:0,
    retention:0,
    sd:StorageDescriptor(
        cols:[
            FieldSchema(name:breed, type:string, comment:null),
            FieldSchema(name:sex, type:string, comment:null)
        ],
        location:hdfs://minmac:9000/user/hive/warehouse/agens_test1,
        inputFormat:null,
        outputFormat:null,
        compressed:false,
        numBuckets:-1,
        serdeInfo:SerDeInfo(
            name:null,
            serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe,
            parameters:{ serialization.format=1 }
        ),
        bucketCols:[],
        sortCols:[],
        parameters:{},
        skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}),
        storedAsSubDirectories:false
    ),
    partitionKeys:[],
    parameters:{
        storage_handler=net.bitnine.agens.hive.AgensHiveStorageHandler,
        agens.graph.datasource=modern,
        agens.conf.livy=http://minmac:8998,
        agens.graph.query=match (a)-[:KNOWS]-(b) return a, b,
        agens.conf.jar=agens-hive-storage-handler-1.0-dev.jar,
        totalSize=0,
        numRows=0,
        rawDataSize=0,
        COLUMN_STATS_ACCURATE={"BASIC_STATS":"true"},
        numFiles=0,
        transient_lastDdlTime=1599440530
    },
    viewOriginalText:null,
    viewExpandedText:null,
    tableType:MANAGED_TABLE,
    rewriteEnabled:false
)


** 비교 : Spark SQL 에서 createTable 로 생성한 테이블
==========================
Detailed Table Information
==========================
Table(
    tableName:temp01,
    dbName:default,
    owner:bgmin,
    createTime:1598244064,
    lastAccessTime:0,
    retention:0,
    sd:StorageDescriptor(
        cols:[
            FieldSchema(name:id, type:bigint, comment:null)
        ],
        location:hdfs://minmac:9000/user/hive/warehouse/temp01,
        inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat,
        outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat,
        compressed:false,
        numBuckets:-1,
        serdeInfo:SerDeInfo(
            name:null,
            serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,
            parameters:{
                serialization.format=1,
                path=hdfs://minmac:9000/user/hive/warehouse/temp01
            }
        ),
        bucketCols:[],
        sortCols:[],
        parameters:{},
        skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}),
        storedAsSubDirectories:false
    ),
    partitionKeys:[],
    parameters:{
        totalSize=935,
        spark.sql.sources.schema.part.0={
            "type":"struct",
            "fields":[
                {"name":"id","type":"long","nullable":true,"metadata":{}}
            ]
        },
        numFiles=2,
        transient_lastDdlTime=1598244064,
        spark.sql.sources.schema.numParts=1,
        spark.sql.sources.provider=parquet,
        spark.sql.create.version=2.4.6
    },
    viewOriginalText:null,
    viewExpandedText:null,
    tableType:MANAGED_TABLE,
    rewriteEnabled:false
)
 */