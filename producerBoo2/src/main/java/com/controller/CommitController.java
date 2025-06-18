package com.controller;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author ming.li
 * @Date 2025/3/18 17:28
 * @Version 1.0
 */
@RestController
@RequestMapping("commit")
public class CommitController {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("commitTrue")
    public String commitTrue()throws Exception{
        for (int i = 0; i <6 ; i++) {
            rocketMQTemplate.convertAndSend("CommitTrueTopic",i);
        }
        return "成功";
    }
    @RequestMapping("commitFalse")
    public String commitFalse()throws Exception{
        for (int i = 0; i <6 ; i++) {
            rocketMQTemplate.convertAndSend("CommitFalseTopic",i);
        }
        return "成功";
    }
}
