package com.davidgeorgehope.mq2kafka;

import com.ibm.mq.*;
import org.apache.kafka.clients.producer.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ibm.mq.constants.CMQC.*;

public class MQ2Kafka {
    protected static Logger logger = Logger.getLogger(MQ2Kafka.class.getName());


    public static void main(String[]args){
        Producer<String, String> producer;

        //final Properties props = loadConfig(args[0]);

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","pkc-ep9mm.us-east-2.aws.confluent.cloud:9092");
        kafkaProps.put("security.protocol","SASL_SSL");
        kafkaProps.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='{{ CLUSTER_API_KEY }}' password='{{ CLUSTER_API_SECRET }}'");
        kafkaProps.put("sasl.mechanism","PLAIN");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        String channel = "";
        String queueManagerName = "";

        Properties properties = new Properties();
        properties.put(CHANNEL_PROPERTY, channel);
        properties.put(TRANSPORT_PROPERTY, TRANSPORT_MQSERIES_BINDINGS);

        try {
            while(true) {

                MQQueueManager queueManager = new MQQueueManager(queueManagerName, properties);
                producer = new KafkaProducer<>(kafkaProps);


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

                    final String key, value;
                    key = "NO-KEY";
                    value = mqMsg;

                    producer.send(new ProducerRecord<String, String>("topic", key, value), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata m, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                logger.log(Level.INFO, "Produced record to topic " + m.topic() + " partition [" + m.partition() + "] @ offset " + m.offset() + "");
                            }
                        }
                    });
                }

                producer.flush();
                producer.close();
                queue.close();
                queueManager.disconnect();
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

}
