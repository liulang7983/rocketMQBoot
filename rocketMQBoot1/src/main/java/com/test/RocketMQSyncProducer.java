package com.test;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Author ming.li
 * @Date 2025/3/17 11:11
 * 同步发送：RocketMQSyncProducer 类实现了同步消息发送。调用 producer.send(msg) 方法发送消息，
 * 该方法会阻塞当前线程，直到接收到服务器的响应，然后返回发送结果
 * @Version 1.0
 */
public class RocketMQSyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("sync_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            // 创建消息实例，指定主题、标签和消息体
            Message msg = new Message("TestTopic", "TagA", ("Sync Hello RocketMQ").getBytes());
            // 同步发送消息
            SendResult sendResult = producer.send(msg);
            System.out.printf("Sync send message result: %s %n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.shutdown();
    }
}
