package com.PullAndPushConsumer;

/**
 * @Author ming.li
 * @Date 2025/3/17 14:54
 * @Version 1.0
 */
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PullConsumerExample {
    public static void main(String[] args) throws MQClientException {
        // 创建拉模式消费者实例，指定消费者组名
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull_consumer_group");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 启动消费者
        consumer.start();

        // 获取主题的所有消息队列
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            // 记录每个消息队列的消费偏移量
            Map<MessageQueue, Long> offsetTable = new HashMap<>();
            try {
                // 获取消息队列的最小偏移量
                long minOffset = consumer.minOffset(mq);
                // 获取消息队列的最大偏移量
                long maxOffset = consumer.maxOffset(mq);
                // 从最小偏移量开始拉取消息
                long offset = minOffset;
                while (true) {
                    // 拉取消息
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(offsetTable, mq), 32);
                    // 处理拉取结果
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            // 处理消息
                            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                            for (int i = 0; i <msgFoundList.size() ; i++) {
                                System.out.println("消息:"+new String(msgFoundList.get(i).getBody()));
                            }
                            System.out.println();
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            // 没有新消息，退出循环
                            break;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                    // 更新消费偏移量
                    putMessageQueueOffset(offsetTable, mq, pullResult.getNextBeginOffset());
                    // 如果没有新消息，退出循环
                    if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 关闭消费者
        consumer.shutdown();
    }

    private static long getMessageQueueOffset(Map<MessageQueue, Long> offsetTable, MessageQueue mq) {
        Long offset = offsetTable.get(mq);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    private static void putMessageQueueOffset(Map<MessageQueue, Long> offsetTable, MessageQueue mq, long offset) {
        offsetTable.put(mq, offset);
    }
}
