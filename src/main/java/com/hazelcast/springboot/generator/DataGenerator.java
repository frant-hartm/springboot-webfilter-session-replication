package com.hazelcast.springboot.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SpringBootApplication
public class DataGenerator implements CommandLineRunner {

    private static final int BATCH_SIZE = 10_000;
    private static final int RECORDS = 10_000_000;
    String[] codes = new String[]{
            "KOMBCZPP",
            "CEKOCZPP",
            "AGBACZPP",
            "CNBACZPP",
            "GIBACZPX",
            "FIOBCZPP",
            "BOTKCZPP",
            "CITFCZPP",
            "MPUBCZPP",
            "ARTTCZPP",
            "POBNCZPP",
            "CTASCZ22",
            "ZUNOCZPP",
            "CITICZPX",
            "BACXCZPP",
            "AIRACZPP",
            "BPKOCZPP",
            "INGBCZPP",
            "SOLACZPP",
            "CMZRCZP1",
            "RZBCCZPP",
            "JTBPCZPP",
            "PMBPCZPP",
            "EQBKCZPP",
            "COBACZPX",
            "BREXCZPP",
            "GEBACZPP",
            "SUBACZPP",
            "DEUTCZ",
            "SPWTCZ21",
            "GENOCZ21",
            "CZEECZPP",
            "MIDLCZPP",
            "PAERCZP1",
            "EEPSCZPP",
            "BKCHCZPP"
    };

    public static void main(String args[]) {
        SpringApplication.run(DataGenerator.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) throws Exception {

        Random random = new Random();

        for (int i = 0; i < RECORDS; i += BATCH_SIZE) {
            List<Object[]> batch = new ArrayList<>();
            for (int j = 0; j < BATCH_SIZE; j++) {
                int amount = random.nextInt(1000) * 1000;

                int sender = random.nextInt(1000) * 1000;
                String senderCode = codes[random.nextInt(codes.length)];

                int receiver = random.nextInt(1000) * 1000;
                String receiverCode = codes[random.nextInt(codes.length)];

                batch.add(new Object[]{i + j, OffsetDateTime.now(), amount, sender, senderCode, receiver,
                        receiverCode, "SEPA"});
            }

            jdbcTemplate.batchUpdate("INSERT INTO " +
                            "transactions (id, timestamp, amount, sender, senderBankCode, receiver, receiverBankCode," +
                            " msgType) " +
                            "VALUES " +
                            "(?, ?, ?, ?, ?, ?, ?, ?)",
                    batch);
        }
    }
}
