package com.hazelcast.springboot.app.http;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.springboot.app.http.repository.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;

@RestController
public class ApiController {

    private TransactionRepository transactionRepository;

    @Autowired HazelcastInstance hz;
    @Autowired AggregationService aggregationService;

    public ApiController(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    @GetMapping("/transaction/{id}")
    @Cacheable("transactions")
    public Transaction transaction(@PathVariable long id) {
        return transactionRepository.getTransaction(id);
    }

    @GetMapping("/transaction/{id}/_nocache")
    public Transaction transactionNoCache(@PathVariable long id) {
        return transactionRepository.getTransaction(id);
    }

    @GetMapping("/transactions/{ids}")
    public Collection<Transaction> transaction(@PathVariable List<Long> ids) {
        return transactionRepository.loadAll(ids);
    }

    @GetMapping("/account/{bankCode}/{accountNumber}/_sum")
    public long accountSum(@PathVariable String bankCode, @PathVariable long accountNumber) {
        return aggregationService.accountSum(accountNumber, bankCode);
    }

    @GetMapping("/account/{bankCode}/{accountNumber}/_sumDb")
    public long accountSumDb(@PathVariable String bankCode, @PathVariable long accountNumber) {
        return transactionRepository.accountSum(accountNumber, bankCode);
    }

    @GetMapping("/cache/_size")
    public int size() {
        return hz.getMap("transactions").size();
    }


}
