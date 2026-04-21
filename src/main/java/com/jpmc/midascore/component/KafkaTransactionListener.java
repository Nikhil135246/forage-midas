package com.jpmc.midascore.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaTransactionListener {

    @Autowired
    private UserRepository userRepo;

    @Autowired
    private TransactionRecordRepository transactionRepo;

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(String message) {

        Transaction tx;

        try {
            ObjectMapper mapper = new ObjectMapper();
            tx = mapper.readValue(message, Transaction.class);

            System.out.println("Parsed: " + tx.getAmount());

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // ✅ fetch users
        UserRecord sender = userRepo.findById(tx.getSenderId());
        UserRecord recipient = userRepo.findById(tx.getRecipientId());

        // ❌ invalid users
        if (sender == null || recipient == null) return;

        // ❌ insufficient balance
        if (sender.getBalance() < tx.getAmount()) return;

        // ✅ update balances
        sender.setBalance(sender.getBalance() - tx.getAmount());
        recipient.setBalance(recipient.getBalance() + tx.getAmount());

        if (sender.getName().equalsIgnoreCase("waldorf")) {
            System.out.println("WALDORF BALANCE: " + sender.getBalance());
        }

        if (recipient.getName().equalsIgnoreCase("waldorf")) {
            System.out.println("WALDORF BALANCE: " + recipient.getBalance());
        }


        // ✅ save users
        userRepo.save(sender);
        userRepo.save(recipient);

        // ✅ save transaction
        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount((double) tx.getAmount());

        transactionRepo.save(record);
    }
}