package com.consumer;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @Author ming.li
 * @Date 2025/3/18 10:50
 * @Version 1.0
 */
@Component
@RocketMQMessageListener(consumerGroup = "MyOrderConsumerGroup", topic = "orderTopic",consumeMode = ConsumeMode.ORDERLY)
public class OrderConsumer implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        System.out.println("thread:"+ Thread.currentThread().getName()+",orderTopic message : "+ message);
    }
}
