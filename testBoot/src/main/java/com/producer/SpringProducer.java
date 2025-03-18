package com.producer;

/**
 * @Author ming.li
 * @Date 2025/3/18 10:49
 * @Version 1.0
 */

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
@Component
public class SpringProducer {

    @Resource
    private RocketMQTemplate rocketMQTemplate;
    //发送普通消息的示例
    public void sendMessage(String topic,String msg){
        this.rocketMQTemplate.convertAndSend(topic,msg);
    }
    //发送事务消息的示例
    public void sendMessageInTransaction(String topic) throws InterruptedException {
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 5; i++) {
            Message<Integer> message = MessageBuilder.withPayload(i).build();
            String destination =topic+":"+tags[i % tags.length];
            System.out.println("Tag:"+destination+"数值:"+i);
            SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(destination, message,destination);
            System.out.printf("%s%n", sendResult);

            Thread.sleep(10);
        }
    }
}
