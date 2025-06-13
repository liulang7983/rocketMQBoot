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
@RocketMQMessageListener(consumerGroup = "TagBConsumerGroup", topic = "tagTopic",selectorExpression = "TagB",consumeMode = ConsumeMode.ORDERLY)
public class TagBConsumer implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        //前面的RocketMQLocalTransactionListener中的executeLocalTransaction返回的是ROLLBACK回滚了
        //这里消费不到
        System.out.println("thread:"+ Thread.currentThread().getName()+",tagTopic TagB message : "+ message);
    }
}
