package com.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author ming.li
 * @Date 2025/6/16 14:25
 * @Version 1.0
 */
@Service
@RocketMQMessageListener(topic = "CommitFalseTopic", consumerGroup = "CommitFalseConsumerGroup", messageModel = MessageModel.CLUSTERING)
public class CommitFalseConsumer implements RocketMQListener<MessageExt> {
    public static AtomicInteger a=new AtomicInteger(0);
    @Override
    public void onMessage(MessageExt message) {
        try {
            System.out.printf("CommitFalseTopic Consumer received message: %s %n", message);
            System.out.println("a的值:"+a);
            if (a.get()<5){
                a.getAndIncrement();
                System.out.println("a加了之后的值:"+a);
                throw new RuntimeException("消息处理失败");
            }
            System.out.println("消费成功:"+new String(message.getBody()));
        }catch (Exception e){
            e.fillInStackTrace();
            throw new RuntimeException("消息处理失败", e);
        }


    }
}



