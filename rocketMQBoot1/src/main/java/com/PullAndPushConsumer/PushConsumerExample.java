package com.PullAndPushConsumer;

/**
 * @Author ming.li
 * @Date 2025/3/17 14:55
 * @Version 1.0
 */
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class PushConsumerExample {
    public static void main(String[] args) throws MQClientException {
        // 创建推模式消费者实例，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("push_consumer_group");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅主题和标签
        consumer.subscribe("TestTopic", "TagA");
        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
