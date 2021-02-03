package com.hazelcast.springboot.app.http;

import com.hazelcast.map.MapLoader;
import com.hazelcast.spring.context.SpringAware;
import com.hazelcast.springboot.app.http.repository.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

@Component
public class TransactionsMapLoader implements MapLoader<Long, Transaction> {

    @Autowired
    private TransactionRepository transactionRepository;

    @Override
    public Transaction load(Long id) {
        return transactionRepository.getTransaction(id);
    }

    @Override
    public Map<Long, Transaction> loadAll(Collection<Long> ids) {
        return transactionRepository.loadAll(ids).stream().collect(toMap(e -> e.getId(), e -> e));
    }

    @Override
    public Iterable<Long> loadAllKeys() {
        return transactionRepository.loadAllKeys();
    }
}
