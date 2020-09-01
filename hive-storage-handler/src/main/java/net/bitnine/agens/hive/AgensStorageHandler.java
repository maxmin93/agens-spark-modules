package net.bitnine.agens.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.CompositeSettings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.mr.security.TokenUtil;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.security.UserProvider;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ClusterInfo;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static net.bitnine.agens.hive.HiveConstants.*;


public class AgensStorageHandler extends DefaultStorageHandler {

    public static void main( String[] args )
    {
        System.out.println( "net.bitnine.agens.hive" );
    }

    private static Log log = LogFactory.getLog(AgensStorageHandler.class);

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return AgensHiveInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return AgensHiveOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return AgensSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        //TODO: add metahook support
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc, true);
        copyToJobProperties(jobProperties, tableDesc.getProperties());
        setUserProviderIfNotSet(jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        init(tableDesc, false);
        copyToJobProperties(jobProperties, tableDesc.getProperties());
        setUserProviderIfNotSet(jobProperties);
    }

    private void setUserProviderIfNotSet(Map<String, String> jobProperties) {
        String key = ConfigurationOptions.ES_SECURITY_USER_PROVIDER_CLASS;
        if (!jobProperties.containsKey(key)) {
            jobProperties.put(key, HadoopUserProvider.class.getName());
        }
    }

    // NB: save the table properties in a special place but nothing else; otherwise the settings might trip on each other
    private void init(TableDesc tableDesc, boolean read) {
        Configuration cfg = getConf();
        // NB: we can't just merge the table properties in, we need to save them per input/output otherwise clashes occur which confuse Hive

        Settings settings = HadoopSettingsManager.loadFrom(cfg);
        //settings.setProperty((read ? HiveConstants.INPUT_TBL_PROPERTIES : HiveConstants.OUTPUT_TBL_PROPERTIES), IOUtils.propsToString(tableDesc.getProperties()));
        if (read) {
            // no generic setting
        }
        else {
            // replace the default committer when using the old API
            HadoopCfgUtils.setOutputCommitterClass(cfg, EsOutputFormat.EsOutputCommitter.class.getName());
        }

        Assert.hasText(tableDesc.getProperties().getProperty(TABLE_LOCATION), String.format(
                "no table location [%s] declared by Hive resulting in abnormal execution;", TABLE_LOCATION));
    }

    private void copyToJobProperties(Map<String, String> jobProperties, Properties properties) {
        // #359, HIVE-8307
        for (String key : properties.stringPropertyNames()) {
            // copy only some properties since apparently job properties can contain junk which messes up the XML serialization
            if (key.startsWith("es.") || key.equals(TABLE_LOCATION) || key.equals(COLUMNS) || key.equals(COLUMNS_TYPES)) {
                jobProperties.put(key, properties.getProperty(key));
            }
        }
    }


    @Override
    @Deprecated
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        if (log.isDebugEnabled()) {
            log.debug("Configuring job credentials for Elasticsearch");
        }
        Settings settings = new CompositeSettings(Arrays.asList(
                HadoopSettingsManager.loadFrom(tableDesc.getProperties()),
                HadoopSettingsManager.loadFrom(jobConf)
        ));
        InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, log);
        UserProvider userProvider = UserProvider.create(settings);
        if (userProvider.isEsKerberosEnabled()) {
            User user = userProvider.getUser();
            ClusterInfo clusterInfo = settings.getClusterInfoOrNull();
            RestClient bootstrap = new RestClient(settings);
            try {
                // first get ES main action info if it's missing
                if (clusterInfo == null) {
                    clusterInfo = bootstrap.mainInfo();
                }
                // Add the token to the job
                TokenUtil.addTokenForJobConf(bootstrap, clusterInfo.getClusterName(), user, jobConf);
            } catch (EsHadoopException ex) {
                throw new EsHadoopIllegalArgumentException(String.format("Cannot detect ES version - "
                        + "typically this happens if the network/Elasticsearch cluster is not accessible or when targeting "
                        + "a WAN/Cloud instance without the proper setting '%s'", ConfigurationOptions.ES_NODES_WAN_ONLY), ex);
            } finally {
                bootstrap.close();
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Ignoring Elasticsearch credentials since Kerberos Auth is not enabled.");
            }
        }
    }

}

/*
CREATE EXTERNAL TABLE agens_test1(
  breed STRING,
  sex STRING
) STORED BY 'net.bitnine.hive.AgensStorageHandler'
TBLPROPERTIES(
  'es.resource'='hive_test',
  'es.nodes'='192.168.0.20:29200',
  'es.net.http.auth.user'='elastic',
  'es.net.http.auth.pass'='bitnine',
  'es.index.auto.create'='true',
  'es.mapping.names'='breed:breed, sex:sex'
)

 */