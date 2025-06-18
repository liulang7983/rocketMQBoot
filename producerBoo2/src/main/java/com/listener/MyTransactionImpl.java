package com.listener;

import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.StringMessageConverter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author ming.li
 * @Date 2025/3/18 16:24
 * @Version 1.0
 */
@RocketMQTransactionListener(rocketMQTemplateBeanName = "rocketMQTemplate")
public class MyTransactionImpl implements RocketMQLocalTransactionListener {

    private ConcurrentHashMap<Object, String> localTrans = new ConcurrentHashMap<>();
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        //发送完就会调用一次，校验本地是否通过，比如存数据库之类的，或者状态校验
        // 如果这里状态未知UNKNOWN，等待稍长时间后还会调用下面的checkLocalTransaction方法
        //当人如果是回滚ROLLBACK则不会调用下面的方法，半消息也不会消费
        Object id = msg.getHeaders().get("id");
        String destination = arg.toString();
        localTrans.put(id,destination);
        org.apache.rocketmq.common.message.Message message = RocketMQUtil.convertToRocketMessage(new StringMessageConverter(),"UTF-8",destination, msg);
        String tags = message.getTags();
        System.out.println("executeLocalTransaction,destination:"+destination+",tags:"+tags);
        if(tags.contains("TagA")){
            return RocketMQLocalTransactionState.COMMIT;
        }else if(tags.contains("TagB")){
            return RocketMQLocalTransactionState.ROLLBACK;
        }else{
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        //SpringBoot的消息对象中，并没有transactionId这个属性。跟原生API不一样。
        //String destination = localTrans.get(msg.getTransactionId());
        Object tags = msg.getPayload();
        MessageHeaders headers = msg.getHeaders();
        String rocketmq_topic =(String) headers.get("rocketmq_TOPIC");
        String rocketmq_tags =(String) headers.get("rocketmq_TAGS");
        System.out.println("checkLocalTransaction topic:"+rocketmq_topic+",tags:"+rocketmq_tags);
        if (rocketmq_tags.contains("tagC")){
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return RocketMQLocalTransactionState.COMMIT;
    }
}

