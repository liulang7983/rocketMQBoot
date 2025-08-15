package com.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Author ming.li
 * @Date 2025/3/18 10:50
 * @Version 1.0
 */
@Component
@RocketMQMessageListener(consumerGroup = "MyOrderConsumerGroup", topic = "orderTopic",consumeMode = ConsumeMode.ORDERLY)
public class OrderConsumer implements RocketMQListener<String> {
    //此时不是多线程消费，只会开一个线程来顺序的从队列拉取数据
    @Override
    public void onMessage(String message) {
        System.out.println("thread:"+ Thread.currentThread().getName()+",orderTopic message : "+ message);
    }
}
