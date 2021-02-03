package com.hazelcast.springboot.app.http;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicates;
import org.springframework.stereotype.Service;

@Service
public class AggregationService {

    private HazelcastInstance hz;

    public AggregationService(HazelcastInstance hz) {
        this.hz = hz;
    }

    public long accountSum(long accountNumber, String bankCode) {

        Long sent = hz.getMap("transactions").aggregate(
                Aggregators.longSum("amount"),
                Predicates.and(
                        Predicates.equal("sender", accountNumber),
                        Predicates.equal("senderBankCode", bankCode)
                )
        );
        Long received = hz.getMap("transactions").aggregate(
                Aggregators.longSum("amount"),
                Predicates.and(
                        Predicates.equal("receiver", accountNumber),
                        Predicates.equal("receiverBankCode", bankCode)
                )
        );
        return received - sent;
    }

}
