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
@RequestMapping("cluster")
public class ClusterController {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("send")
    public String send()throws Exception{
        for (int i = 0; i <6 ; i++) {
            rocketMQTemplate.convertAndSend("clusterTopic",i);
        }
        return "成功";
    }
}
