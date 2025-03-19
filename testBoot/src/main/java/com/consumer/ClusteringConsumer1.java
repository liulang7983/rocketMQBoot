package com.consumer;

/**
 * @Author ming.li
 * @Date 2025/3/18 17:22
 * @Version 1.0
 */
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(topic = "clusterTopic", consumerGroup = "clusteringConsumerGroup",
        messageModel = MessageModel.CLUSTERING)
public class ClusteringConsumer1 implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        System.out.printf("Clustering Consumer received message: %s %n", message);
    }
}
