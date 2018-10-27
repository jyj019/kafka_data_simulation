package com.calabar.loadbyte;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;


public class SimpleKafkaProducer {
    public static void main(String[] args) throws Exception{

        //Assign topicName to string variable
        String topicName = "test5";
        // create instance for properties to access producer configs  log.cleanup.policy
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
    

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "kafkaUtils.EncodeingKafka");
//	      props.put("partitioner.class", "继承了Partition的类，实现的是根据指定的算法把消息推送到指定的分区中com.ys.test.SpringBoot.zktest.util.MyPartition");

        Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
        long startTimes = System.currentTimeMillis();
        System.out.println();

        byte[] bytes=prouderByte();
//	    	  producer.send(new ProducerRecord<String, Object>(topicName,Integer.toString(i),asList));
//	          producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(i), perSon));
        while (true) {
            producer.send(new ProducerRecord<String, Object>(topicName, "hhhhhh:1538185042", bytes), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println("  发送成功：" + "checksum: " + metadata.checksum() + " offset: " + metadata.offset() + " partition: " + metadata.partition() + " topic: " + metadata.topic());
                    }
                    if (exception != null) {
                        System.out.println("异常：" + exception.getMessage());
                    }
                }
            });
            Thread.sleep(5000);
        }

    }

    public static byte[] prouderByte()  {
        StringBuffer str = new StringBuffer();
        str.append("5657000000CC010300C8");
        for (int i=1;i<=50;i++){
            switch (i){
                case 1:case 2: case 5: case 7: case 8:case 10:case 11:case 13:case 14:case 16:case 17:case 19: case 20: case 22: case 23:case 25:
                    //case 51:case 52: case 55: case 57: case 58:case 60:case 61:case 63:case 64:case 66:case 67:case 69: case 70: case 72: case 73:case 75:
                    str.append("05F6");
                    break;
                case 50: //case 100:
                    str.append("0001");
                    break;
                default:
                    str.append("02D4");
                    break;
            }
        }
        for (int i=1;i<=50;i++){
            switch (i){
                case 1:case 2: case 5: case 7: case 8:case 10:case 11:case 13:case 14:case 16:case 17:case 19: case 20: case 22: case 23:case 25:
                    //case 51:case 52: case 55: case 57: case 58:case 60:case 61:case 63:case 64:case 66:case 67:case 69: case 70: case 72: case 73:case 75:
                    str.append("0041");
                    break;
                case 50: //case 100:
                    str.append("011D");
                    break;
                default:
                    str.append("0000");
                    break;
            }
        }
        String hex=String .valueOf(str);
        byte[] bytes=hexStringToByte( hex);

//        System.out.println(hex);
//        System.out.println("***********************************************************************");
//
//        String[] strings= AnalysisMerge.run(bytes);
//
//        System.out.println(strings[0]+"\n"+strings[1]);
        return  bytes;
    }

    /**
     * 16进制字符串转换成字节数组
     *
     * @param hex
     * @return
     */
    public static byte[] hexStringToByte(String hex) {
        byte[] b = new byte[hex.length() / 2];
        int j = 0;
        for (int i = 0; i < b.length; i++) {
            char c0 = hex.charAt(j++);
            char c1 = hex.charAt(j++);
            b[i] = (byte) ((parse(c0) << 4) | parse(c1));
        }
        return b;
    }

    private static int parse(char c) {
        if (c >= 'a')
            return (c - 'a' + 10) & 0x0f;
        if (c >= 'A')
            return (c - 'A' + 10) & 0x0f;
        return (c - '0') & 0x0f;
    }
}