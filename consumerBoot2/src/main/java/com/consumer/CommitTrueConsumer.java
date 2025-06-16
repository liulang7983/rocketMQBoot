package com.consumer;

import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * @Author ming.li
 * @Date 2025/6/16 14:25
 * @Version 1.0
 */
@Service
@RocketMQMessageListener(topic = "CommitTrueTopic", consumerGroup = "CommitTrueConsumerGroup",
        messageModel = MessageModel.CLUSTERING)
public class CommitTrueConsumer implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        System.out.printf("CommitTrueTopic Consumer received message: %s %n", message);
    }
}



