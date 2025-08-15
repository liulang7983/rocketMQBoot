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
        System.out.println(Thread.currentThread().getName()+" 等待消费:"+new String(message.getBody()));
        System.out.printf("CommitFalseTopic Consumer received message: %s %n", message);
        System.out.println(Thread.currentThread().getName()+" a的值:"+a.get());
        if (a.get()%5==0){
            a.getAndIncrement();
            System.out.println(Thread.currentThread().getName()+" a加了之后的值:"+a.get());
            //不能try/catch,否则就算自动提交了
            throw new RuntimeException("消息处理失败");
        }
        System.out.println(Thread.currentThread().getName()+" 消费成功:"+new String(message.getBody()));
    }
}



