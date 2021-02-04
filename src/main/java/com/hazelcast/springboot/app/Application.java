package com.hazelcast.springboot.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.springboot.app.http.TransactionPortableFactory;
import com.hazelcast.springboot.app.http.TransactionsMapLoader;
import com.hazelcast.springboot.app.http.repository.TransactionRepository;
import com.hazelcast.web.WebFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastInstanceFactory;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.AbstractMap.SimpleImmutableEntry;
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

        transactions.addIndexConfig(new IndexConfig(IndexType.HASH, "senderBankCode", "sender"));
        transactions.addIndexConfig(new IndexConfig(IndexType.HASH, "receiverBankCode", "receiver"));

        config.getSerializationConfig().addPortableFactory(1, new TransactionPortableFactory());
        return config;
    }

    // Jet on CP disables hz autoconfiguration in hazelcast-spring
    @Bean
    HazelcastInstance hazelcastInstance(Config config) {
        return (new HazelcastInstanceFactory(config)).getHazelcastInstance();
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
        IMap<Object, Object> transactions = hz.getMap("transactions");
        if (transactions.isEmpty()) {
            log.info("Loading transactions using Pipeline");

            JetInstance jet = hz.getComputeEngine().getJet();
            Pipeline p = Pipeline.create();
            p.readFrom(Sources.jdbc(
                    () -> DriverManager.getConnection("jdbc:postgresql://172.20.0.2/postgres?user=postgres&password=postgres"),
                    (c, parallelism, index) -> {
                        PreparedStatement stmt = c.prepareStatement("SELECT * FROM transactions WHERE MOD(id, ?) = ?");
                        stmt.setInt(1, parallelism);
                        stmt.setInt(2, index);
                        return stmt.executeQuery();
                    },
                    TransactionRepository::readTransactionFromRS))
             .map(t -> new SimpleImmutableEntry<>(t.getId(), t))
             .writeTo(Sinks.map("transactions"));
            jet.newJob(p).join();
            log.info("Load data finished");
        } else {
            log.info("transactions map contains records, not loading anything");
        }

    }
}
