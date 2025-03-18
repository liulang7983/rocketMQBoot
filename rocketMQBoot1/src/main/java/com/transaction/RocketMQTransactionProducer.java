package com.transaction;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;
/**
 * @Author ming.li
 * @Date 2025/3/18 9:50
 * @Version 1.0
 */


public class RocketMQTransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建事务消息生产者实例，指定生产者组名
        TransactionMQProducer producer = new TransactionMQProducer("transaction_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");

        // 设置事务监听器
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.printf("Executing local transaction for message: %s %n", new String(msg.getBody()));
                // 模拟本地事务执行，这里简单返回未知状态，让 Broker 回查(后续会调用checkLocalTransaction再次确认状态)
                //return LocalTransactionState.UNKNOW;
                //这个代表成功，会直接表示可以让消费者消费了(后续不会调用checkLocalTransaction再次确认状态)
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.printf("Checking local transaction for message: %s %n", new String(msg.getBody()));
                // 模拟检查本地事务结果，返回提交状态(executeLocalTransaction返回UNKNOW的情况会调用他再次确认)
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        // 启动生产者
        producer.start();

        try {
            // 创建消息实例，指定主题、标签和消息体
            Message msg = new Message("TransactionTopic", "TagA", ("Transaction Message").getBytes());
            // 发送事务消息
            TransactionSendResult sendResult = producer.sendMessageInTransaction(msg, null);
            System.out.printf("Send Result: %s %n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 等待一段时间，确保事务检查完成
        TimeUnit.SECONDS.sleep(100);
        // 关闭生产者
        producer.shutdown();
    }
}
