package com.controller;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author ming.li
 * @Date 2025/3/18 17:28
 * @Version 1.0
 */
@RestController
@RequestMapping("tag")
public class TagController {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("send")
    public String send()throws Exception{
        for (int i = 0; i <6 ; i++) {
            if (i%2==0){
                rocketMQTemplate.convertAndSend("tagTopic:tagA",i);
            }else {
                rocketMQTemplate.convertAndSend("tagTopic:tagB",i);
            }

        }
        return "成功";
    }
    @RequestMapping("sendOrder")
    public String sendOrder()throws Exception{
        for (int i = 0; i <6 ; i++) {
            Message message = new Message();
            if (i%2==0){
                message=new Message("tagTopic","tagA","0",("消息:"+i).getBytes());
            }else {
                message=new Message("tagTopic","tagB","0",("消息:"+i).getBytes());
            }
            rocketMQTemplate.getProducer().send(message);

        }
        return "成功";
    }
    @RequestMapping("sendTransaction")
    public String sendTransaction()throws Exception{
        for (int i = 0; i <6 ; i++) {
            Message message = new Message();
            String str="";
            if (i%2==0){
                str="tagTopic:TagA";
                message=new Message("tagTopic","TagA","0",("消息:"+i).getBytes());
            }else {
                str="tagTopic:TagB";
                message=new Message("tagTopic","TagB","0",("消息:"+i).getBytes());
            }
            org.springframework.messaging.Message<Message> message1 = MessageBuilder.withPayload(message).build();
            rocketMQTemplate.sendMessageInTransaction(str,message1,str);
            if (i%3==0){
                str="tagTopic:TagC";
                message=new Message("tagTopic","TagC","0",("消息:"+i).getBytes());

            }else {
                str="tagTopic:TagD";
                message=new Message("tagTopic","TagD","0",("消息:"+i).getBytes());
            }
            org.springframework.messaging.Message<Message> message2 = MessageBuilder.withPayload(message).build();
            rocketMQTemplate.sendMessageInTransaction(str,message2,str);
        }
        return "成功";
    }
}
