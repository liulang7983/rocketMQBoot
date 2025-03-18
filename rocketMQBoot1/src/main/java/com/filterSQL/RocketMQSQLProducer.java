package com.filterSQL;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
/**
 * @Author ming.li
 * @Date 2025/3/18 9:10
 * @Version 1.0
 */


public class RocketMQSQLProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("sql_producer_group");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        try {
            // 发送消息并设置属性
            Message msg1 = new Message("SQLFilterTopic", "Tag", "Message with age 20".getBytes());
            msg1.putUserProperty("age", "20");
            SendResult result1 = producer.send(msg1);
            System.out.printf("Send message 1 result: %s %n", result1);

            Message msg2 = new Message("SQLFilterTopic", "Tag", "Message with age 30".getBytes());
            msg2.putUserProperty("age", "30");
            SendResult result2 = producer.send(msg2);
            System.out.printf("Send message 2 result: %s %n", result2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
