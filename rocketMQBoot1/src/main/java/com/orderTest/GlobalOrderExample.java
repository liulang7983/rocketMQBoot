package com.orderTest;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * @Author ming.li
 * @Date 2025/3/20 15:16
 * @Version 1.0
 */
public class GlobalOrderExample {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 启动生产者
        startProducer();
        // 启动消费者
        startConsumer();
    }

    private static void startProducer() throws MQClientException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("GlobalOrderProducerGroup");
        // 指定 NameServer 地址
        //producer.setNamesrvAddr("localhost:9876");
        producer.setNamesrvAddr("127.0.0.1:9876;127.0.0.1:9877;127.0.0.1:9878;127.0.0.1:9879");
        // 启动生产者
        producer.start();

        // 使用单线程执行器发送消息
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    // 创建消息实例，指定主题、标签和消息体
                    Message msg = new Message("GlobalOrderTopic", "TagA", ("Hello, Global Order " + index).getBytes());
                    // 选择固定队列，这里选择第一个队列
                    SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            return mqs.get(0);
                        }
                    }, null);
                    System.out.printf("%s%n", sendResult);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        try {
            //使用的线程池，需要等待执行完成后再关闭执行器和生产者，所以需要休眠
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭执行器
        executorService.shutdown();
        // 关闭生产者
        producer.shutdown();
    }

    private static void startConsumer() throws MQClientException {
        // 创建消费者实例，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GlobalOrderConsumerGroup");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅主题和标签
        consumer.subscribe("GlobalOrderTopic", "TagA");
        // 注册顺序消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                System.out.println("拉去任务条数:"+msgs.size());
                for (MessageExt msg : msgs) {

                    System.out.printf("Received global ordered message: %s%n  messageId :%s%n", new String(msg.getBody()),msg.getMsgId());
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
