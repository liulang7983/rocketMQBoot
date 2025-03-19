package com.consumer;

import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * @Author ming.li
 * @Date 2025/3/19 11:21
 * @Version 1.0
 */

@Service
@RocketMQMessageListener(topic = "broadcastingTopic", consumerGroup = "broadcastingConsumerGroup",
        messageModel = MessageModel.BROADCASTING)
public class BroadcastingConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        System.out.printf("broadcasting Consumer received message: %s %n", message);
    }
}
