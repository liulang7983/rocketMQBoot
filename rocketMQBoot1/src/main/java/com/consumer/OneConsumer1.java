package com.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author ming.li
 * @Date 2025/3/13 16:44
 * @Version 1.0
 */
public class OneConsumer1 {
    public static void main(String[] args) throws Exception {
        // 1. 创建 DefaultMQPushConsumer 实例，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // 2. 设置 NameServer 地址
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 3. 订阅主题和标签，* 表示订阅该主题下的所有标签
        consumer.subscribe("TopicTest", "*");

        // 4. 注册顺序消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        // 处理消息
                        System.out.println(msg.getBrokerName()+":"+Thread.currentThread().getName()+":"+new String(msg.getBody()));
                    } catch (Exception e) {
                        e.printStackTrace();
                        // 消费失败，稍后重试
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                // 消费成功
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        // 5. 启动消费者
        consumer.start();
        System.out.printf("Ordered Consumer Started.%n");
    }

}
