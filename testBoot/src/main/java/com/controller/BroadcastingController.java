package com.controller;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author ming.li
 * @Date 2025/3/19 11:20
 * @Version 1.0
 */

@RestController
@RequestMapping("broadcasting")
public class BroadcastingController {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("send")
    public String send()throws Exception{
        for (int i = 0; i <6 ; i++) {
            rocketMQTemplate.convertAndSend("broadcastingTopic",i);
        }
        return "成功";
    }
}
