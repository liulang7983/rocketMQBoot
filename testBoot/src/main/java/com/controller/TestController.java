package com.controller;

import com.producer.SpringProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author ming.li
 * @Date 2025/3/18 16:27
 * @Version 1.0
 */
@RestController
@RequestMapping("test")
public class TestController {
    @Autowired
    private SpringProducer springProducer;

    @RequestMapping("send")
    public String send(String topic)throws Exception{
        springProducer.sendMessageInTransaction(topic);
        return "成功";
    }
}
