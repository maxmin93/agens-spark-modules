package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.HiveBytesArrayWritable;
import org.elasticsearch.hadoop.hive.HiveBytesConverter;
import org.elasticsearch.hadoop.hive.HiveValueWriter;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.rest.InitializationUtils;

import java.io.IOException;
import java.util.Properties;


public class AgensHiveOutputFormat extends EsOutputFormat implements HiveOutputFormat {

    static class EsHiveRecordWriter extends EsOutputFormat.EsRecordWriter implements FileSinkOperator.RecordWriter {

        private final Progressable progress;

        public EsHiveRecordWriter(Configuration cfg, Progressable progress) {
            super(cfg, progress);
            this.progress = progress;
        }

        @Override
        public void write(Writable w) throws IOException {
            if (!initialized) {
                initialized = true;
                init();
            }

            if (w instanceof HiveBytesArrayWritable) {
                HiveBytesArrayWritable hbaw = ((HiveBytesArrayWritable) w);
                repository.writeProcessedToIndex(hbaw.getContent());
            }
            else {
                // we could allow custom BAs
                throw new EsHadoopIllegalArgumentException(String.format("Unexpected type; expected [%s], received [%s]", HiveBytesArrayWritable.class, w));
            }
        }

        @Override
        public void close(boolean abort) throws IOException {
            // TODO: check whether a proper Reporter can be passed in
            super.doClose(progress);
        }
    }

    public EsHiveRecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass, boolean isCompressed, Properties tableProperties, Progressable progress) {
        // force the table properties to be merged into the configuration
        // NB: the properties are also available in HiveConstants#OUTPUT_TBL_PROPERTIES
        Settings settings = HadoopSettingsManager.loadFrom(jc).merge(tableProperties);

        Log log = LogFactory.getLog(getClass());

        // NB: ESSerDe is already initialized at this stage but should still have a reference to the same cfg object
        // NB: the value writer is not needed by Hive but it's set for consistency and debugging purposes

        InitializationUtils.setValueWriterIfNotSet(settings, HiveValueWriter.class, log);
        InitializationUtils.setBytesConverterIfNeeded(settings, HiveBytesConverter.class, log);
        InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, log);

        // set write resource
        settings.setResourceWrite(settings.getResourceWrite());

        HiveUtils.init(settings, log);

        return new EsHiveRecordWriter(jc, progress);
    }
}
