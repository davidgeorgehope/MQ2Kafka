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

        //run with ./mq2kafka kafka.properties mq.properties

        Producer<String, String> producer;

        String topic;

        Properties kafkaProps = loadConfig(args[0]);
        if(kafkaProps == null) {
            topic="TEST";
            kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "pkc-ep9mm.us-east-2.aws.confluent.cloud:9092");
            kafkaProps.put("security.protocol", "SASL_SSL");
            kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{{ CLUSTER_API_KEY }}' password='{{ CLUSTER_API_SECRET }}'");
            kafkaProps.put("sasl.mechanism", "PLAIN");
            kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }else{
            topic = kafkaProps.getProperty("topic");
            kafkaProps.remove("topic");
            kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }


        String channel;
        String queueManagerName;
        String queueName;
        Properties mqProps = null;

        if (args.length>1) {
             mqProps = loadConfig(args[1]);
        }
        if(mqProps == null) {
            mqProps = new Properties();
            mqProps.put(CHANNEL_PROPERTY, "SYSTEM.DEF.SVRCONN");
            queueManagerName = "QMA";
            queueName = "QUEUE1";
            mqProps.put(TRANSPORT_PROPERTY, TRANSPORT_MQSERIES_BINDINGS);

        }else{
            channel = mqProps.getProperty(CHANNEL_PROPERTY);
            String transport = mqProps.getProperty(TRANSPORT_PROPERTY);
            queueManagerName = mqProps.getProperty("QUEUE_MANAGER");
            queueName = mqProps.getProperty("QUEUE");

            mqProps = new Properties();
            mqProps.put(CHANNEL_PROPERTY, channel);
            mqProps.put(TRANSPORT_PROPERTY, transport);
        }
        logger.log(Level.INFO, "START");

        try {

            MQQueue queue;
            MQQueueManager queueManager;

            int openOptions = MQC.MQOO_INPUT_AS_Q_DEF;

            queueManager = new MQQueueManager(queueManagerName, mqProps);
            queue = queueManager
                    .accessQueue(queueName,
                            openOptions, null, null, null);

            producer = new KafkaProducer<>(kafkaProps);


            try {

                while (true) {

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

                        producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {
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

                }


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    producer.close();
                    queue.close();
                    queueManager.disconnect();
                } catch (Exception e) {

                }
            }
        }
        catch(Exception e){
            e.printStackTrace();


        }

    }

    public static Properties loadConfig(final String configFile)  {
        try {
            if (!Files.exists(Paths.get(configFile))) {
                throw new IOException(configFile + " not found.");
            }
            final Properties cfg = new Properties();
            try (InputStream inputStream = new FileInputStream(configFile)) {
                cfg.load(inputStream);
            }
            return cfg;
        }catch(Exception e){
            return null;
        }
    }

}
