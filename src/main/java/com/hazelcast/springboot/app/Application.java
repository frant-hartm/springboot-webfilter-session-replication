package com.hazelcast.springboot.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.springboot.app.http.TransactionPortableFactory;
import com.hazelcast.springboot.app.http.TransactionsMapLoader;
import com.hazelcast.web.WebFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.util.Properties;

@SpringBootApplication
@EnableCaching
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    @Autowired
    private HazelcastInstance hz;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public Config config(TransactionsMapLoader transactionsMapLoader) {
        Config config = new Config();

        MapConfig transactions = new MapConfig("transactions");
        MapStoreConfig transactionsStoreConfig = transactions.getMapStoreConfig();
        transactionsStoreConfig.setEnabled(true);
        transactionsStoreConfig.setImplementation(transactionsMapLoader);
        transactionsStoreConfig.setInitialLoadMode(InitialLoadMode.EAGER);
        config.addMapConfig(transactions);

        transactions.addIndexConfig(new IndexConfig(IndexType.HASH, "senderBankCode", "sender"));
        transactions.addIndexConfig(new IndexConfig(IndexType.HASH, "receiverBankCode", "receiver"));

        config.getSerializationConfig().addPortableFactory( 1, new TransactionPortableFactory() );
        return config;
    }

    @Bean
    public WebFilter webFilter(HazelcastInstance hazelcastInstance) {

        Properties properties = new Properties();
        properties.put("instance-name", hazelcastInstance.getName());
        properties.put("sticky-session", "false");

        return new WebFilter(properties);
    }

    @PostConstruct
    public void createTransactionMap() {
        log.info("Create transactions map, loading all keys");
        hz.getMap("transactions");
        log.info("Transactions map loaded");
    }
}
