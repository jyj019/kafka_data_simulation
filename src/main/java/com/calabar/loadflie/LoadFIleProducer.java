package com.calabar.loadflie;

import com.calabar.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class LoadFIleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadFIleProducer.class);
    static StringBuffer buffer = new StringBuffer();


public static Map<String, Object> loadConfig(String confPath) throws Exception {
        Map<String, Object> kafkaConfg = new HashMap<>();
        Properties props = new Properties();
        try {

            if (!confPath.endsWith(File.separator)) {
                confPath = confPath + File.separator;
            }
            confPath = confPath + File.separator + "dec-esc.properties";
            props.load(new FileInputStream(confPath));
            Iterator<Map.Entry<Object, Object>> it = props.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Object, Object> tmp = it.next();
                String key = String.valueOf(tmp.getKey());
                Object value = tmp.getValue();
                if (key.startsWith(KafkaUtils.KAFKA_CONFIG_PREFIX)) {
                    kafkaConfg.put(key, value);
                }
            }
            return kafkaConfg;
        } catch (IOException e) {
            LOGGER.error("配置文件加载路径不正确!" + confPath, e);
            throw new Exception("配置文件加载路径不正确!" + confPath, e);
        }
    }

    public static void main(String[] args) throws Exception {
        //System.out.println(prouderJson());
        //String json=prouderJson();

        String topic = args[0];
        int sleepTime = 5000;
        String confPath = args[1];
//        if (args != null || args.length == 3) {
//            topic = args[0];
//            sleepTime = Integer.valueOf(args[1]);
////            path = args[2];
//        }
        KafkaUtils kafka = new KafkaUtils();
        Map config = loadConfig(confPath);
        kafka.initProducer(config);

        String path= args[2];
        String fileName="data.json";
        loadParam(path,fileName);
        while (true) {
            Integer msgId = Math.toIntExact(System.currentTimeMillis() / 1000);
           // System.out.println(buffer.toString());
            kafka.sendMsg(buffer.toString(), topic);
          //  kafka.sendMsg("11111111", topic);
//            if (msgId == msgId + 300 * 10000) {
//                break;
//            }
            System.out.println(msgId);
            Thread.sleep(sleepTime);
        }
    }

    /**
     * Init local param.
     *
     * @throws IOException the io exception
     */
    public static void loadParam(String path,String fileName) throws IOException {
        //String path = System.getProperty("PROJECT_HOME");
        if(!path.endsWith(File.separator)){
            path = path+File.separator;
        }
        String filePath = path + fileName;
        FileInputStream inputStream = new FileInputStream(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String str = null;
        while ((str = bufferedReader.readLine()) != null) {
            buffer.append(str);
        }
        inputStream.close();
        bufferedReader.close();
    }
}
