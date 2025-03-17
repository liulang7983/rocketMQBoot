package com.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author ming.li
 * @Date 2025/3/13 16:44
 * MessageListenerConcurrently多线程消费不保证顺序性
 * @Version 1.0
 */
public class OneConsumer {
    public static void main(String[] args) throws Exception {
        // 1. 创建 DefaultMQPushConsumer 实例，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // 2. 设置 NameServer 地址
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 3. 订阅主题和标签，* 表示订阅该主题下的所有标签
        consumer.subscribe("TopicTest", "*");

        // 4. 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        // 处理消息
                        System.out.println(Thread.currentThread().getName() + ":" + new String(msg.getBody()));
                    } catch (Exception e) {
                        e.printStackTrace();
                        // 消费失败，稍后重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                // 消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 5. 启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
