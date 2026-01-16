package com.kafkasinglenode;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafKaProducerCallBack implements Callback {
    String messageKey;

    //Set message key to identify message.
    public KafKaProducerCallBack(String messageKey) {
        super();
        this.messageKey = messageKey;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            System.out.println("Exception for : "
                    + "Message Key = " + messageKey + " : " + e.getMessage());
        } else {
            System.out.println(" Callback received for Message Key " + messageKey
                    + " returned Partition : " + recordMetadata.partition()
                    + " and Offset : " + recordMetadata.offset());
        }

    }
}