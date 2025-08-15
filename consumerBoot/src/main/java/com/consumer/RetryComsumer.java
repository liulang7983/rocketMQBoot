package com.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author ming.li
 * @Date 2025/3/20 10:01
 * @Version 1.0
 */
@Service
@RocketMQMessageListener(topic = "retryTopic", consumerGroup = "TestConsumerGroup")
public class RetryComsumer implements RocketMQListener<String> {

    public static AtomicInteger a=new AtomicInteger(0);

    @Override
    public void onMessage(String message) {
        try {
            a.getAndIncrement();
            // 模拟业务处理异常，触发重试
            if (a.get() % 3==0) {
                throw new RuntimeException("Consume message failed, retry " + message);
            }
            System.out.println("Message consumed successfully: " + message);
        } catch (Exception e) {
            System.out.println("Consume message failed: " + e.getMessage());
            // 抛出异常会触发 RocketMQ 重试机制
            throw e;
        }
    }
}