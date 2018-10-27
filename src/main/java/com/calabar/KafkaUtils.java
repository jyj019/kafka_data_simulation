package com.calabar;


import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    public static final String KAFKA_CONFIG_PREFIX = "kafka.";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "kafka.bootstrap.servers";
    public static final String GROUP_ID_CONFIG = "kafka.group.id";
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "kafka.key.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "kafka.value.deserializer";
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "kafka.heartbeat.interval.ms";
    public static final String SESSION_TIMEOUT_MS_CONFIG = "kafka.session.timeout.ms";
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "kafka.enable.auto.commit";
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "kafka.auto.commit.interval.ms";
    public static final String AUTO_OFFSET_RESET_CONFIG = "kafka.auto.offset.reset";
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "kafka.connections.max.idle.ms";
    public static final String MAX_POLL_RECORDS_CONFIG = "kafka.max.poll.records";
    public static final String CLIENT_ID_CONFIG = "kafka.client.id";
    public static final String ACKS_CONFIG = "kafka.acks";
    public static final String RETRIES_CONFIG = "kafka.retries";
    public static final String LINGER_MS_CONFIG = "kafka.linger.ms";
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "kafka.key.serializer";
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "kafka.value.serializer";
    public static final String MAX_BLOCK_MS_CONFIG = "kafka.max.block.ms";
    public static final String BUFFER_MEMORY_CONFIG = "kafka.buffer.memory";
    public static final String COMPRESSION_TYPE_CONFIG = "kafka.compression.type";
    public static final String BATCH_SIZE_CONFIG = "kafka.batch.size";
    public static final String MAXRATEPERPARTITION_CONFIG = "kafka.maxRatePerPartition";
    public static final String SEND_BUFFER_CONFIG = "kafka.send.buffer.bytes";
    public static final String MAX_REQUEST_SIZE_CONFIG = "kafka.max.request.size";

    /**
     * 流程输出的数据组织 json 字符串
     */
    @Getter
    private Producer<String, String> producer;

    @Getter
    private KafkaConsumer<String, String> consumer;

    @Getter
    /** KafkaProducer */
    private static KafkaUtils INSTANCE;

    /**
     * instance 的getter方法
     *
     * @return 返回 instance
     */
    public static synchronized KafkaUtils getInstance() throws Exception {
        if (null == INSTANCE) {
            INSTANCE = new KafkaUtils();
            LOGGER.info("初始化 kafka producer...");
        }
        return INSTANCE;
    }

    public void initProducer(Map<String, Object> props) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS_CONFIG));
        config.put(ProducerConfig.ACKS_CONFIG, props.get(ACKS_CONFIG));
        config.put(ProducerConfig.RETRIES_CONFIG, props.get(RETRIES_CONFIG));
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, props.get(BATCH_SIZE_CONFIG));
        config.put(ProducerConfig.LINGER_MS_CONFIG, props.get(LINGER_MS_CONFIG));
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, props.get(BUFFER_MEMORY_CONFIG));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.get(KEY_SERIALIZER_CLASS_CONFIG));
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.get(VALUE_SERIALIZER_CLASS_CONFIG));
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, props.get(SEND_BUFFER_CONFIG));
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, props.get(MAX_REQUEST_SIZE_CONFIG));
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + System.currentTimeMillis());
        producer = new KafkaProducer<>(config);
    }


    /**
     * kafka consumer 初始化配置
     *
     * @return
     */
    public void initConsumer(Map<Object, Object> props) {
        Properties config = new Properties();
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.get(AUTO_OFFSET_RESET_CONFIG));
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.get(MAX_POLL_RECORDS_CONFIG));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, props.get(GROUP_ID_CONFIG));
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS_CONFIG));
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, props.get(SESSION_TIMEOUT_MS_CONFIG));
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, props.get(VALUE_DESERIALIZER_CLASS_CONFIG));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, props.get(KEY_DESERIALIZER_CLASS_CONFIG));
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, props.get(AUTO_COMMIT_INTERVAL_MS_CONFIG));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, props.get(ENABLE_AUTO_COMMIT_CONFIG));
        consumer = new KafkaConsumer<>(config);
    }

//    public void send(JedisCluster jedis,String topic,String key) {
//        String msg = jedis.lpop(key);
//        if(StringUtils.isNotBlank(msg)){
//            try {
//                sendMsg(topic,"",  msg);
//            } catch (Exception e) {
//                // 放回数据
//                jedis.lpush(key,msg);
//                LOGGER.warn("数据重放",e);
//            }
//        }
//    }


    public void sendMsg(String msg, String topic) {
        try {
            sendMsg(topic, "", msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送消息
     *
     * @param key key
     * @param msg msg
     * @param msg msg
     * @return true:发送成功，false:发送失败
     * @throws Exception Exception
     */
    public boolean sendMsg(String topic, String key, String msg)/* throws Exception */ {
        boolean isDone = false;
        if (StringUtils.isNotEmpty(msg)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);
            Future<RecordMetadata> fu = this.producer.send(record);
            try {
                RecordMetadata rm = fu.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

            /*Future<RecordMetadata> fu = this.producer.send(record, (RecordMetadata, Exception) -> {
                if (null != Exception) {
                    retrySendMessage(record);
                }
            });
            try {
                RecordMetadata rm = fu.get();
                isDone = null != rm;
            } catch (Exception e) {
                LOGGER.error("重试多次仍不能发送数据");
                throw new Exception("重试多次仍不能发送数据");
            }
        } else {
            LOGGER.warn("发送的数据为空！");
        }*/

        return isDone;
    }

    /**
     * 当kafka消息发送失败后,重试
     *
     * @param record ProducerRecord<String, String="String"></String,>
     */
    private void retrySendMessage(final ProducerRecord<String, String> record) {
        boolean isDone = false;
        int retryTime = 0;
        do {
            LOGGER.warn("正在重试第 " + retryTime + " 次发送数据到Kafka . . . .");
            Future<RecordMetadata> fu = this.producer.send(record);
            try {
                RecordMetadata rm = fu.get();
                isDone = null != rm;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            retryTime++;
        } while (!isDone && retryTime > 5);
    }
}
