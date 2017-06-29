package io.openmessaging.demo;

/**
 * Created by mst on 2017/5/14.
 */

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProducer  implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();
    Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        WriteToDisk.loadThreadToStart();
        MessageStore.activeThread.put("active",MessageStore.activeThread.getOrDefault("active", 0) + 1);
    }


    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {
        if (message == null)
            throw new ClientOMSException("Message should not be null");
        KeyValue msgHeader = message.headers();
        String topic = msgHeader.getString(MessageHeader.TOPIC);
        String queue = msgHeader.getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        String topicOrQueue = topic == null ? queue : topic;
        try{
            messageStore.putMessage(topicOrQueue,  message);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
    /**
     * 为比赛新增的flush接口，评测线程会最后调用该接口；
     * 选手在接口里应该把缓存中的数据写入磁盘或者pagecache
     * 在规定时间内，该接口没有返回，producer会被强制杀掉，可能会有数据丢失，从而导致数据不正确；
     */
    @Override public void flush(){
        MessageStore.activeThread.put("active", MessageStore.activeThread.get("active") - 1);
        if(MessageStore.activeThread.get("active") == 0)
            messageStore.flush();
    }
}

