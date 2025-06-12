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
        //当消息发送到 RocketMQ Broker 失败时，生产者会自动重试发送，最多重试 3 次。
        // 这个配置对于保证消息的可靠性发送非常重要，特别是在网络不稳定的环境中
        rocketMQTemplate.getProducer().setRetryTimesWhenSendFailed(3);
        //异步发送失败重试次数
        rocketMQTemplate.getProducer().setRetryTimesWhenSendAsyncFailed(3);
        // 发送消息到指定主题
        rocketMQTemplate.convertAndSend("retryNumberTopic", message);
        return "Message sent successfully";
    }
}
