package net.bitnine.agens.hive;

import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.mapred.AvroJob.OUTPUT_CODEC;
import static org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroGenericRecordWriter;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class AgensHiveInputFormat implements HiveOutputFormat<WritableComparable, AvroGenericRecordWritable> {

    public static final Log LOG = LogFactory.getLog(AgensHiveInputFormat.class);

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Path path, Class<? extends Writable> valueClass, boolean isCompressed,
            Properties properties, Progressable progressable
    ) throws IOException {

        Schema schema;
        try {
            schema = AvroSerdeUtils.determineSchemaOrThrowException(jobConf, properties);
        } catch (AvroSerdeException e) {
            throw new IOException(e);
        }
        GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(gdw);

        dfw.create(schema, path.getFileSystem(jobConf).create(path));
        return new AvroGenericRecordWriter(dfw);
    }

    class WrapperRecordWriter<K extends Writable, V extends Writable> implements RecordWriter<K, V> {
        FileSinkOperator.RecordWriter hiveWriter = null;
        JobConf jobConf;
        Progressable progressable;
        String fileName;

        public WrapperRecordWriter(JobConf jobConf, Progressable progressable, String fileName) {
            this.progressable = progressable;
            this.jobConf = jobConf;
            this.fileName = fileName;
        }

        private FileSinkOperator.RecordWriter getHiveWriter() throws IOException {
            if (this.hiveWriter == null) {
                Properties properties = new Properties();
                for (AvroSerdeUtils.AvroTableProperties tableProperty : AvroSerdeUtils.AvroTableProperties.values()) {
                    String propVal;
                    if ((propVal = jobConf.get(tableProperty.getPropName())) != null) {
                        properties.put(tableProperty.getPropName(), propVal);
                    }
                }

                Boolean isCompressed = jobConf.getBoolean("mapreduce.output.fileoutputformat.compress", false);
                Path path = new Path(this.fileName);
                if (path.getFileSystem(jobConf).isDirectory(path)) {
                    // This path is only potentially encountered during setup
                    // Otherwise, a specific part_xxxx file name is generated and passed in.
                    path = new Path(path, "_dummy");
                }

                this.hiveWriter = getHiveRecordWriter(jobConf, path, null, isCompressed, properties, progressable);
            }
            return this.hiveWriter;
        }

        @Override
        public void write(K key, V value) throws IOException {
            getHiveWriter().write(value);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            // Normally, I'd worry about the blanket false being passed in here, and that
            // it'd need to be integrated into an abort call for an OutputCommitter, but the
            // underlying recordwriter ignores it and throws it away, so it's irrelevant.
            getHiveWriter().close(false);
        }

    }

    //no records will be emitted from Hive
    @Override
    public RecordWriter<WritableComparable, AvroGenericRecordWritable>
    getRecordWriter(FileSystem ignored, JobConf job, String fileName,
                    Progressable progress) throws IOException {
        return new WrapperRecordWriter<WritableComparable, AvroGenericRecordWritable>(job, progress, fileName);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        return; // Not doing any check
    }

}

/*
bin/hive

add jar file:///Users/bgmin/Workspaces/spark/agens-spark-connector/target/agens-spark-connector-full-1.0-dev.jar;

create temporary function agensUDF as 'net.bitnine.hive.udf.AgensUDF';

select agensUDF('TEST') as msg;

CREATE EXTERNAL TABLE ages_test1(
    >   breed STRING,
    >   sex STRING
    > ) STORED BY 'net.bitnine.hive.AgensStorageHandler'
    > TBLPROPERTIES(
    >   'es.resource'='hive_test',
    >   'es.nodes'='192.168.0.20:29200',
    >   'es.net.http.auth.user'='elastic',
    >   'es.net.http.auth.pass'='bitnine',
    >   'es.index.auto.create'='true',
    >   'es.mapping.names'='breed:breed, sex:sex'
    > );
OK
Time taken: 0.057 seconds

select * from ages_test1;
 */