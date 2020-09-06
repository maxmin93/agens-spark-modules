package net.bitnine.agens.hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;

abstract class HiveUtils {
/*
    // HCatUtil method
    public static HiveStorageHandler getStorageHandler(Configuration conf, String storageHandler, String serDe, String inputFormat, String outputFormat) throws IOException {
        if ((storageHandler == null) || (storageHandler.equals(FosterStorageHandler.class.getName()))) {
            try {
                FosterStorageHandler fosterStorageHandler = new FosterStorageHandler(inputFormat, outputFormat, serDe);
                fosterStorageHandler.setConf(conf);
                return fosterStorageHandler;
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to load " + "foster storage handler", e);
            }
        }
        try {
            Class<? extends HiveStorageHandler> handlerClass = (Class<? extends HiveStorageHandler>) Class.forName(storageHandler, true, Utilities.getSessionSpecifiedClassLoader());
            return (HiveStorageHandler) ReflectionUtils.newInstance(handlerClass, conf);
        } catch (ClassNotFoundException e) {
            throw new IOException("Error in loading storage handler." + e.getMessage(), e);
        }
    }
*/
}
