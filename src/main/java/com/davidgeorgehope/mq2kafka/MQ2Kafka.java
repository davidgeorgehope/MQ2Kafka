package com.davidgeorgehope.mq2kafka;

import com.ibm.mq.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

import static com.ibm.mq.constants.CMQC.*;

public class MQ2Kafka {

    static Producer<String, String> producer;

    public static void main(String[]args){

        Properties kafkaProps = new Properties();
        producer = new KafkaProducer<>(kafkaProps);

        Properties properties = new Properties();
        properties.put(CHANNEL_PROPERTY, "channel");
        properties.put(TRANSPORT_PROPERTY, TRANSPORT_MQSERIES_BINDINGS);
        try {
            MQQueueManager queueManager = new MQQueueManager("Yeah", properties);

            int openOptions = MQC.MQOO_INPUT_AS_Q_DEF
                    | MQC.MQOO_INPUT_SHARED;

            MQQueue queue = queueManager
                    .accessQueue("",
                            openOptions, null, null, null);
                int depth = queue.getCurrentDepth();
                while (depth-- > 0) {
                    MQMessage msg = new MQMessage();
                    MQGetMessageOptions gmo = new MQGetMessageOptions();
                    queue.get(msg, gmo);
                    String mqMsg = msg.readStringOfByteLength(msg.getDataLength());
                    produce(mqMsg);
                }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Future<RecordMetadata> produce(final String message) {
        final String[] parts = message.split("-");
        final String key, value;
        if (parts.length > 1) {
            key = parts[0];
            value = parts[1];
        } else {
            key = "NO-KEY";
            value = parts[0];
        }
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("", key, value);
        return producer.send(producerRecord);
    }
}
