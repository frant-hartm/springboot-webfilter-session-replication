package com.hazelcast.springboot.app.http;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class Transaction implements Portable {

    public static final int CLASS_ID = 1;
    long id;
    OffsetDateTime timestamp;
    long amount;

    long sender;
    String senderBankCode;

    long receiver;
    String receiverBankCode;
    String msgType;

    public Transaction() {
    }

    public Transaction(
            long id,
            OffsetDateTime timestamp,
            long amount,
            long sender,
            String senderBankCode,
            long receiver,
            String receiverBankCode,
            String msgType
    ) {
        this.id = id;
        this.timestamp = timestamp;
        this.amount = amount;
        this.sender = sender;
        this.senderBankCode = senderBankCode;
        this.receiver = receiver;
        this.receiverBankCode = receiverBankCode;
        this.msgType = msgType;
    }

    public long getId() {
        return id;
    }

    public OffsetDateTime getTimestamp() {
        return timestamp;
    }

    public long getAmount() {
        return amount;
    }

    public long getSender() {
        return sender;
    }

    public String getSenderBankCode() {
        return senderBankCode;
    }

    public Long getReceiver() {
        return receiver;
    }

    public String getReceiverBankCode() {
        return receiverBankCode;
    }

    public String getMsgType() {
        return msgType;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("id", id);
        writer.writeLong("timestamp", timestamp.toInstant().toEpochMilli());
        writer.writeLong("amount", amount);
        writer.writeLong("sender", sender);
        writer.writeUTF("senderBankCode", senderBankCode);
        writer.writeLong("receiver", receiver);
        writer.writeUTF("receiverBankCode", receiverBankCode);
        writer.writeUTF("msgType", msgType);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        id = reader.readLong("id");
        timestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(reader.readLong("timestamp")), ZoneOffset.UTC);
        amount = reader.readLong("amount");
        sender = reader.readLong("sender");
        senderBankCode = reader.readUTF("senderBankCode");
        receiver = reader.readLong("receiver");
        receiverBankCode = reader.readUTF("receiverBankCode");
        msgType = reader.readUTF("msgType");
    }
}
