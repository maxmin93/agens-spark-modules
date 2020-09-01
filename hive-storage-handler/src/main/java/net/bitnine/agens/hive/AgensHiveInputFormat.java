package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.HiveValueReader;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static net.bitnine.agens.hive.HiveConstants.TABLE_LOCATION;


public class AgensHiveInputFormat extends EsInputFormat<Text, Writable> {

    static class EsHiveSplit extends FileSplit {
        InputSplit delegate;
        private Path path;

        EsHiveSplit() {
            this(new EsInputSplit(), null);
        }

        EsHiveSplit(InputSplit delegate, Path path) {
            super(path, 0, 0, (String[]) null);
            this.delegate = delegate;
            this.path = path;
        }

        public long getLength() {
            // TODO: can this be delegated?
            return 1L;
        }

        public String[] getLocations() throws IOException {
            return delegate.getLocations();
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, path.toString());
            delegate.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            path = new Path(Text.readString(in));
            delegate.readFields(in);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }

    @Override
    public FileSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        // first, merge input table properties (since there's no access to them ...)
        Settings settings = HadoopSettingsManager.loadFrom(job);
        //settings.merge(IOUtils.propsFromString(settings.getProperty(HiveConstants.INPUT_TBL_PROPERTIES)));

        Log log = LogFactory.getLog(getClass());

        // move on to initialization
        InitializationUtils.setValueReaderIfNotSet(settings, HiveValueReader.class, log);
        InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, log);
        if (settings.getOutputAsJson() == false) {
            // Only set the fields if we aren't asking for raw JSON
            settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS, StringUtils.concatenate(HiveUtils.columnToAlias(settings), ","));
        }

        HiveUtils.init(settings, log);

        // decorate original splits as FileSplit
        InputSplit[] shardSplits = super.getSplits(job, numSplits);
        FileSplit[] wrappers = new FileSplit[shardSplits.length];
        Path path = new Path(job.get(TABLE_LOCATION));
        for (int i = 0; i < wrappers.length; i++) {
            wrappers[i] = new EsHiveSplit(shardSplits[i], path);
        }
        return wrappers;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public AbstractWritableEsInputRecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) {
        InputSplit delegate = ((EsHiveSplit) split).delegate;
        return isOutputAsJson(job) ? new JsonWritableEsInputRecordReader(delegate, job, reporter) : new WritableEsInputRecordReader(delegate, job, reporter);
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