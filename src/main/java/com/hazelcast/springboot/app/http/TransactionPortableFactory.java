package com.hazelcast.springboot.app.http;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class TransactionPortableFactory implements PortableFactory {

    @Override
    public Portable create(int classId) {
        if (classId == Transaction.CLASS_ID) {
            return new Transaction();
        } else {
            return null;
        }
    }
}
