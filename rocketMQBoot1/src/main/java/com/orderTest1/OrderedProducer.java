package com.orderTest1;

/**
 * @Author ming.li
 * @Date 2025/6/10 14:05
 * @Version 1.0
 */
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class OrderedProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建生产者实例并指定组名
        DefaultMQProducer producer = new DefaultMQProducer("OrderedProducerGroup");
        // 指定NameServer地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            // 模拟多个订单，每个订单有多个步骤
            for (int orderId = 1; orderId <= 3; orderId++) {
                // 每个订单有4个步骤（支付、发货、配送、签收）
                for (int step = 1; step <= 4; step++) {
                    String body = "OrderID=" + orderId + ", Step=" + step;
                    Message msg = new Message("OrderedTopic", "OrderTag", body.getBytes());

                    // 通过MessageQueueSelector实现消息按订单ID分配到同一队列
                    SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            // 根据订单ID选择队列（使用取模算法）
                            Integer id = (Integer) arg;
                            int index = id % mqs.size();
                            return mqs.get(index);
                        }
                    }, orderId); // 传递订单ID作为选择队列的参数

                    System.out.printf("发送结果: %s, 队列: %s, 消息: %s%n",
                            sendResult.getSendStatus(),
                            sendResult.getMessageQueue().getQueueId(),
                            body);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭生产者
            producer.shutdown();
        }
    }
}
