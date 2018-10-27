package com.calabar.simple;


import com.calabar.KafkaUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;


public class SimpleConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    public static Map<Object, Object> loadConfig(String confPath) throws Exception {
        Properties props = new Properties();
        try {
            String path = System.getProperty("PROJECT_HOME");
            if (StringUtils.isNotBlank(path)) {
                confPath = path;
                LOGGER.info("配置文件加载路径-->" + path);
            }
            if(!confPath.endsWith(File.separator)){
                confPath = confPath+File.separator;
            }
            confPath = confPath + "dec-esc.properties";
            props.load(new FileInputStream(confPath));
            return props;
        } catch (IOException e) {
            LOGGER.error("配置文件加载路径不正确!" + confPath, e);
            throw new Exception("配置文件加载路径不正确!" + confPath, e);
        }
    }

    public static void main(String[] args) throws Exception {
        KafkaUtils kafkaUtils = new KafkaUtils();
        Map config = loadConfig(args[0]);

        System.out.println(config);
        kafkaUtils.initConsumer(config);
        KafkaConsumer<String, String> consumer = kafkaUtils.getConsumer();
//        consumer.subscribe(Arrays.asList("Test_2"));
        consumer.subscribe(Arrays.asList("Test_1"));
//        List<TopicPartition> topicPartitionList= new ArrayList<>();
//        topicPartitionList.add(new TopicPartition("Test_3",0));
//        consumer.seekToBeginning(topicPartitionList);
//        consumer.assign(topicPartitionList);
        ConsumerRecords<String, String> consumerRecords = null;
        System.out.println("********start********");
        while (true) {
            consumerRecords = consumer.poll(1000);
            Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator();
            while (it.hasNext()) {
             //   System.out.println(it.next().key());
                System.out.println(it.next().value());
            }
            Thread.sleep(50000);
        }
    }
}
