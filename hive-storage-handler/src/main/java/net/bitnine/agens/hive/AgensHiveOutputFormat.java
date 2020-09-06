package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.avro.AvroGenericRecordReader;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AgensHiveOutputFormat extends FileInputFormat<NullWritable, AvroGenericRecordWritable>  implements JobConfigurable {

    public static final Log LOG = LogFactory.getLog(AgensHiveOutputFormat.class);

    protected JobConf jobConf;

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        for (FileStatus file : super.listStatus(job)) {
            result.add(file);
        }
        return result.toArray(new FileStatus[0]);
    }

    @Override
    public RecordReader<NullWritable, AvroGenericRecordWritable>
    getRecordReader(InputSplit inputSplit, JobConf jc, Reporter reporter) throws IOException {
        return new AvroGenericRecordReader(jc, (FileSplit) inputSplit, reporter);
    }

    @Override
    public void configure(JobConf jobConf) {
        this.jobConf = jobConf;
    }

}
