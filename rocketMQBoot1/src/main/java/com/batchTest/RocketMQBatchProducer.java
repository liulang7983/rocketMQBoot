package com.batchTest;

/**
 * @Author ming.li
 * @Date 2025/3/17 17:10
 * @Version 1.0
 */
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class RocketMQBatchProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("batch_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            // 创建消息列表用于批量发送
            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                // 创建消息实例，指定主题、标签和消息体
                Message msg = new Message("BatchTopic", "TagA", ("Batch Message " + i).getBytes());
                messages.add(msg);
            }

            // 批量发送消息
            SendResult sendResult = producer.send(messages);
            System.out.printf("Send Result: %s %n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.shutdown();
    }
}
