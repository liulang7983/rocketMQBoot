package com.orderTest;

/**
 * @Author ming.li
 * @Date 2025/3/17 16:05
 * @Version 1.0
 */
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RocketMQOrderedConsumer {
    public static void main(String[] args) throws MQClientException {
        // 创建消费者实例，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ordered_consumer_group");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅主题和标签
        consumer.subscribe("OrderedTopic", "TagA");
        // 注册顺序消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            Random random = new Random();

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    // 模拟处理消息的耗时
                    try {
                        TimeUnit.SECONDS.sleep(random.nextInt(2));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.printf("%s Receive New Messages: %s  queueId : %s %n", Thread.currentThread().getName(), new String(msg.getBody()),msg.getQueueId());
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
