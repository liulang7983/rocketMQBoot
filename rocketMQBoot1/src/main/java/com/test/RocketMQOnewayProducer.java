package com.test;

/**
 * @Author ming.li
 * @Date 2025/3/17 11:11
 * 单向发送：RocketMQOnewayProducer 类实现了单向消息发送。调用 producer.sendOneway(msg) 方法，
 * 该方法不会等待服务器的响应，直接返回，不关心消息是否发送成功
 * @Version 1.0
 */
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class RocketMQOnewayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("oneway_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            // 创建消息实例，指定主题、标签和消息体
            Message msg = new Message("TestTopic", "TagA", ("Oneway Hello RocketMQ").getBytes());
            // 单向发送消息(该方法没有返回值)
            producer.sendOneway(msg);
            System.out.println("Oneway send message");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.shutdown();
    }
}
