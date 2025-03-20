package com.delayTest;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
/**
 * @Author ming.li
 * @Date 2025/3/20 14:40
 * @Version 1.0
 */
public class RocketMQDelayQueueExample {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 启动生产者
        startProducer();
        // 启动消费者
        startConsumer();
    }

    private static void startProducer() throws MQClientException {
        // 初始化生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("DelayProducerGroup");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            // 创建消息实例，指定主题、标签和消息体
            Message msg = new Message("DelayTopic", "TagA", "Hello, RocketMQ Delay Queue".getBytes());
            // 设置延迟级别为 3，代表延迟 10s
            msg.setDelayTimeLevel(3);
            // 发送消息
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.shutdown();
    }

    private static void startConsumer() throws MQClientException {
        // 初始化消费者实例，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("DelayConsumerGroup");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅主题和标签
        consumer.subscribe("DelayTopic", "TagA");
        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("Received message: %s, delay time: %dms%n", new String(msg.getBody()), msg.getStoreTimestamp() - msg.getBornTimestamp());
                }
                // 消费成功，返回消费状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
