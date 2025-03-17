package com.test;

/**
 * @Author ming.li
 * @Date 2025/3/17 11:05
 * @Version 1.0
 */
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class RocketMQProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            for (int i = 0; i < 10; i++) {
                // 创建消息实例，指定主题、标签和消息体
                Message msg = new Message("TestTopic", "TagA", ("Hello RocketMQ " + i).getBytes());
                // 发送消息
                producer.send(msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.shutdown();
    }
}
