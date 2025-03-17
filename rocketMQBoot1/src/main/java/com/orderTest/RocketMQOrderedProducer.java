package com.orderTest;

/**
 * @Author ming.li
 * @Date 2025/3/17 16:05
 * @Version 1.0
 */
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

public class RocketMQOrderedProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("ordered_producer_group");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        try {
            for (int i = 0; i < 10; i++) {
                // 创建消息实例，指定主题、标签和消息体
                Message msg = new Message("OrderedTopic", "TagA", ("Ordered Message " + i).getBytes());
                // 顺序发送消息，根据业务规则选择消息队列
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        // 这里简单地根据消息序号选择队列
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        //每次都推送到同一个queueId上，就会是顺序发送，对于该队列所有数据就会顺序消费
                        index=0;
                        return mqs.get(index);
                    }
                }, i);
                System.out.printf("Send Result: %s %n", sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.shutdown();
    }
}
