package com.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * @Author ming.li
 * @Date 2025/3/20 10:01
 * @Version 1.0
 */
@Service
@RocketMQMessageListener(topic = "retryTopic", consumerGroup = "TestConsumerGroup")
public class RetryComsumer implements RocketMQListener<String> {

    private int retryCount = 0;

    @Override
    public void onMessage(String message) {
        try {
            // 模拟业务处理异常，触发重试
            if (retryCount < 3) {
                retryCount++;
                throw new RuntimeException("Consume message failed, retry " + retryCount + " times");
            }
            System.out.println("Message consumed successfully: " + message);
        } catch (Exception e) {
            System.out.println("Consume message failed: " + e.getMessage());
            // 抛出异常会触发 RocketMQ 重试机制
            throw e;
        }
    }
}
