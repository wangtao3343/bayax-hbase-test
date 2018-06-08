package com.bayax.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;


@Configuration
public class HBaseConfigurationBase {
    private Logger logger= LoggerFactory.getLogger(HBaseConfigurationBase.class);

    @Value("${hadoop.home.dir}")
    private String hadoop_home;

    @Bean
    public Connection configuration() {

        String os = System.getProperties().getProperty("os.name");
        if (os != null && os.toLowerCase().indexOf("linux") == -1) {
            System.setProperty("hadoop.home.dir", hadoop_home);
        }

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        // 重新设置或者读取hbase-site.xml
        //conf.set("hbase.zookeeper.quorum","bayax-hdp01.hadoop,bayax-hdp02.hadoop,bayax-hdp03.hadoop");
        //conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
