package com.controller;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

/**
 * @Author ming.li
 * @Date 2025/3/20 10:00
 * @Version 1.0
 */
@RestController
@RequestMapping("order")
public class OrderController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("/send")
    public String sendMessage()throws Exception {
        String str = "Hello, retryNumberTopic:";
        // 发送消息到指定主题
        for (int i = 0; i <6 ; i++) {
            Message message = new Message("orderTopic", (str+i).getBytes(StandardCharsets.UTF_8));
            // 指定发送到的队列
            SendResult sendResult = rocketMQTemplate.getProducer().send(message, (mqs, msg, arg) -> mqs.get((Integer) arg), 0);
        }
        return "Message sent successfully";
    }

    @RequestMapping("test")
    public String send()throws Exception{
        for (int i = 0; i <6 ; i++) {
            Message message = new Message("TestTopic", ("TestTopic"+i).getBytes(StandardCharsets.UTF_8));
            // 指定发送到的队列
            SendResult sendResult = rocketMQTemplate.getProducer().send(message, (mqs, msg, arg) -> mqs.get((Integer) arg), 0);
        }
        return "成功";
    }
}
