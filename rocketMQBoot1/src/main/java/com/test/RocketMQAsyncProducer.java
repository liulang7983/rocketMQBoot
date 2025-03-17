package com.test;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @Author ming.li
 * @Date 2025/3/17 11:10
 * 异步发送：RocketMQAsyncProducer 类实现了异步消息发送。调用 producer.send(msg, sendCallback) 方法，
 * 该方法不会阻塞当前线程，而是在消息发送完成后通过回调函数 SendCallback 处理发送结果
 * @Version 1.0
 */
public class RocketMQAsyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("async_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();
        try {
            // 创建消息实例，指定主题、标签和消息体
            Message msg = new Message("TestTopic", "TagA", ("Async Hello RocketMQ").getBytes());
            // 异步发送消息
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("Async send message success: %s %n", sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("Async send message failed: %s %n", e);
                }
            });
            System.out.println("发送完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 等待异步操作完成
        Thread.sleep(5000);
        // 关闭生产者
        producer.shutdown();
    }
}
