package com.orderTest1;

/**
 * @Author ming.li
 * @Date 2025/6/10 14:05
 * @Version 1.0
 */
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class OrderedConsumer {
    public static void main(String[] args) throws MQClientException {
        // 创建消费者实例并指定组名（同一组内的消费者负载均衡消费）
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("OrderedConsumerGroup");
        // 指定NameServer地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅主题和标签（*表示所有标签）
        consumer.subscribe("OrderedTopic", "OrderTag");

        // 注册顺序消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            private Random random = new Random();

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                // 设置自动提交
                context.setAutoCommit(true);

                for (MessageExt msg : msgs) {
                    try {
                        // 模拟业务处理耗时
                        TimeUnit.MILLISECONDS.sleep(random.nextInt(100));

                        // 打印消息内容和队列ID（同一订单的消息应在同一队列）
                        System.out.printf("线程: %s, 队列ID: %d, 消息: %s%n",
                                Thread.currentThread().getName(),
                                msg.getQueueId(),
                                new String(msg.getBody()));
                    } catch (Exception e) {
                        e.printStackTrace();
                        // 消费失败，返回SUSPEND_CURRENT_QUEUE_A_MOMENT，稍后重试
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                // 消费成功
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        // 启动消费者
        consumer.start();
        System.out.println("消费者已启动");
    }
}
