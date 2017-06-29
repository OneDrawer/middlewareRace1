package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    Logger logger = LoggerFactory.getLogger(DefaultPullConsumer.class);
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        ReadDisk.loadThreadToStart();
        //use Round Robin

        while(!(ReadDisk.loadCompleted && MessageStore.getInstance().getRawBucket().size() == 0)) {
            int checkNum = 0;
            int lastIndex = 0;
            int len = bucketList.size();
            while (++checkNum <= len) {
                String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
                Message message = messageStore.pullMessage(queue, bucket);
                if (message != null) {
                    return message;
                }
            }
        }
        return null;

    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have already attached to a queue " + queue);
        }
        ConcurrentHashMap<String, Integer> addConsumer = messageStore.numOfConsumer;
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);

        clearConsumer();
        bucketList.clear();
        bucketList.addAll(buckets);
        int len = bucketList.size();
        for(int i = 0; i < len; i++) {
            String bucket = bucketList.get(i);
            addConsumer.put(bucket, addConsumer.getOrDefault(bucket, 0) + 1);
            logger.info("add " + bucket + " to messageStore, bucket size = " + messageStore.numOfConsumer.get(bucket));
        }
        logger.info("attach queue success!");
    }
    void clearConsumer() {
        int len = bucketList.size();
        for(int i = 0; i < len; i++) {
            String bucket = bucketList.get(i);
            messageStore.numOfConsumer.put(bucket, messageStore.numOfConsumer.getOrDefault(bucket, 0) - 1);
        }
    }


}
