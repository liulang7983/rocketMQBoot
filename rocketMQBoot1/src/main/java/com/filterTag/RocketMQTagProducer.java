package com.filterTag;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Author ming.li
 * @Date 2025/3/18 9:04
 * @Version 1.0
 */


public class RocketMQTagProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("tag_producer_group");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        try {
            // 发送带有 TagA 的消息
            Message msg1 = new Message("TagFilterTopic", "TagA", "This is a message with TagA".getBytes());
            SendResult result1 = producer.send(msg1);
            System.out.printf("Send TagA message result: %s %n", result1);

            // 发送带有 TagB 的消息
            Message msg2 = new Message("TagFilterTopic", "TagB", "This is a message with TagB".getBytes());
            SendResult result2 = producer.send(msg2);
            System.out.printf("Send TagB message result: %s %n", result2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
