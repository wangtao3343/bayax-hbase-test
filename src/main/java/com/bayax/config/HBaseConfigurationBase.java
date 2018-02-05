package com.bayax.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.util.Properties;


@org.springframework.context.annotation.Configuration
public class HBaseConfigurationBase {
    private Logger logger= LoggerFactory.getLogger(HBaseConfigurationBase.class);

    @Value("${hadoop.home.dir}")
    private String hadoop_home;

    @Bean
    public Configuration configuration() {

        if (!isOSLinux()) {
            System.setProperty("hadoop.home.dir", hadoop_home);
        }

        Configuration conf = HBaseConfiguration.create();

        // 重新设置或者读取hbase-site.xml
        //conf.set("hbase.zookeeper.quorum","bayax-hdp01.hadoop,bayax-hdp02.hadoop,bayax-hdp03.hadoop");
        //conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        return conf;
    }

    public static boolean isOSLinux() {
        Properties prop = System.getProperties();

        String os = prop.getProperty("os.name");
        if (os != null && os.toLowerCase().indexOf("linux") > -1) {
            return true;
        } else {
            return false;
        }
    }
}
