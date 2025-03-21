package com.controller;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author ming.li
 * @Date 2025/3/20 10:00
 * @Version 1.0
 */
@RestController
@RequestMapping("retryNumber")
public class RetryNumberController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("/send")
    public String sendMessage() {
        String message = "Hello, retryNumberTopic!";
        rocketMQTemplate.getProducer().setRetryTimesWhenSendFailed(3);
        rocketMQTemplate.getProducer().setRetryTimesWhenSendAsyncFailed(3);
        // 发送消息到指定主题
        rocketMQTemplate.convertAndSend("retryNumberTopic", message);
        return "Message sent successfully";
    }
}
