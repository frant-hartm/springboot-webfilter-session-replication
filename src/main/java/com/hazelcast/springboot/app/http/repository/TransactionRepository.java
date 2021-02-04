package com.hazelcast.springboot.app.http.repository;

import com.hazelcast.springboot.app.http.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;

@Repository
public class TransactionRepository {

    private static final Logger log = LoggerFactory.getLogger(TransactionRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public TransactionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    public Transaction getTransaction(long id) {

        log.debug("getTransaction id={}", id);

        Transaction tx = jdbcTemplate.query("SELECT * from transactions WHERE id = ?",
                (ResultSet rs) -> {
                    boolean next = rs.next();
                    if (next) {
                        return readTransactionFromRS(rs);
                    } else {
                        return null;
                    }
                },
                id);

        return tx;
    }

    public Collection<Transaction> loadAll(Collection<Long> ids) {
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("ids", ids);

        return namedParameterJdbcTemplate.query("SELECT * from transactions WHERE id IN (:ids)",
                parameters,
                (rs, num) -> readTransactionFromRS(rs)
        );
    }

    public static Transaction readTransactionFromRS(ResultSet rs) throws SQLException {
        return new Transaction(
                rs.getLong("id"),
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(rs.getTimestamp("timestamp").getTime()),
                        ZoneOffset.UTC),
                rs.getLong("amount"),
                rs.getLong("sender"),
                rs.getString("senderBankCode"),
                rs.getLong("receiver"),
                rs.getString("receiverBankCode"),
                rs.getString("msgType"));
    }

    public Collection<Long> loadAllKeys() {
        log.info("loadAllKeys");
        return jdbcTemplate.queryForList("SELECT id from transactions LIMIT 100",
                Long.class
        );
    }

    public long accountSum(long accountNumber, String bankCode) {
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("accountNumber", accountNumber);
        parameters.addValue("bankCode", bankCode);

        Long sum = namedParameterJdbcTemplate.queryForObject("SELECT received.sum - sent.sum " +
                        "FROM (SELECT sum(amount) AS sum FROM transactions WHERE senderbankcode = :bankCode AND " +
                        "sender = :accountNumber) AS sent " +
                        "         cross join " +
                        "     (SELECT sum(amount) AS sum FROM transactions WHERE receiverbankcode = :bankCode AND " +
                        "receiver = :accountNumber) AS received",
                parameters,
                Long.class);
        if (sum == null) {
            return 0;
        } else {
            return sum;
        }
    }
}
