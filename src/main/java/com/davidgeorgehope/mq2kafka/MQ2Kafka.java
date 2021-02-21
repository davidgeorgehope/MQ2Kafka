package com.davidgeorgehope.mq2kafka;

import com.ibm.mq.*;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ibm.mq.constants.CMQC.*;

public class MQ2Kafka {
    protected static Logger logger = Logger.getLogger(MQ2Kafka.class.getName());

    static Producer<String, String> producer;

    public static void main(String[]args){

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","pkc-ep9mm.us-east-2.aws.confluent.cloud:9092");
        kafkaProps.put("security.protocol","SASL_SSL");
        kafkaProps.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='{{ CLUSTER_API_KEY }}' password='{{ CLUSTER_API_SECRET }}'");
        kafkaProps.put("sasl.mechanism","PLAIN");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

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
                    logger.log(Level.INFO, mqMsg);
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
