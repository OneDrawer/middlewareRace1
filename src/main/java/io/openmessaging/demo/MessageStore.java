package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.tester.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class MessageStore {
    public static ConcurrentHashMap<String, Integer> activeThread = new ConcurrentHashMap();
    private static final MessageStore INSTANCE = new MessageStore();
    private static Logger logger= LoggerFactory.getLogger(MessageStore.class);
    public static byte[] br ="\n".getBytes();
    public static byte[] space = " ".getBytes();
    public static byte[] colon = ":".getBytes();
    public static byte[] vert = "|".getBytes();
    //******************************************************************************||
    //****************************  Config Information *****************************||
    //******************************************************************************||
    //public static String STORE_PATH = "result" + "/";
    static String STORE_PATH = Constants.STORE_PATH;
    // basic
    public final static int SIZE_OF_ARRAY = 400;

    public final static int BUFFER_SIZE = 3;

    /********************************************************************************/
    /********************************************************************************/

    public static ConcurrentHashMap<String, ConcurrentHashMap> currentRead = new ConcurrentHashMap<>();
    public  static ConcurrentHashMap<String, Integer> numOfConsumer = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<FileSlice, Integer> remainReadConsumer = new ConcurrentHashMap<>();
    //TMP　
    public static MessageStore getInstance() {
        return INSTANCE;
    }
    static ConcurrentHashMap<String, Integer> fileSplit = new ConcurrentHashMap<>();
    public static Object lockerForReadControl = new Object();

    public  Map<String, ArrayList<Message>> messageBuckets = new ConcurrentHashMap<>();
    public  Map<FileSlice, ArrayList<String>> rawBuckets = new ConcurrentHashMap<>();
    private Map<String, ConcurrentHashMap<String, Integer>> queueOffsets = new ConcurrentHashMap<>();
    HashMap<String, Object> producerLock = new HashMap<>();
    static final LinkedList<ByteBuffer> availBufferList = new LinkedList<>();
    static final LinkedList<ByteBuffer> fullBufferList = new LinkedList<>();
    static {
        for(int i = 0; i < BUFFER_SIZE; i++) {
            availBufferList.add(ByteBuffer.allocateDirect(22 * 1024 * 1024));
        }
    }
    public Map<FileSlice, ArrayList<String>> getRawBucket() {
        return rawBuckets;
    }

    public  void putMessage(String bucket, Message message) throws Exception {
        ArrayList<Message> bucketList;
        ArrayList<Message> fullBucketList ;
        Object locked;
        synchronized (this){
            locked = producerLock.get(bucket);
            if(locked == null) {
                locked = new Object();
                producerLock.put(bucket, locked);
            }
        }
        synchronized (locked) {
            bucketList = messageBuckets.get(bucket);
            if(bucketList == null) {
                bucketList = new ArrayList<>();
                messageBuckets.put(bucket, bucketList);
            }
            bucketList.add(message);

            if (bucketList.size() >= SIZE_OF_ARRAY) {
                fullBucketList = messageBuckets.get(bucket);
                messageBuckets.put(bucket, new ArrayList<>());
                int slice = fileSplit.getOrDefault(bucket, 0);
                fileSplit.put(bucket, slice + 1);
                ByteBuffer currentByteBuffer;
                synchronized (availBufferList) {
                    while (availBufferList.size() == 0) {
                        try {
                            availBufferList.wait();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    currentByteBuffer = availBufferList.getFirst();
                    availBufferList.remove(currentByteBuffer);
                }
                mergeMessage(bucket, slice, fullBucketList, currentByteBuffer);
            }
        }
    }
    public static  void mergeMessage(String bucket, int slice, ArrayList<Message> al,ByteBuffer buffer) {
        int len = al.size();
        DefaultBytesMessage writeMsg;
        buffer.put(bucket.getBytes());
        buffer.put(br);
        buffer.put((Integer.toString(slice)).getBytes());
        buffer.put(br);
        buffer.put((Integer.toString(len)).getBytes());
        buffer.put(br);
        for(int i = 0; i < len; i++) {
            writeMsg = (DefaultBytesMessage)al.get(i);
            KeyValue headers = writeMsg.headers();
            for(String key: headers.keySet()) {
                buffer.put(key.getBytes());
                buffer.put(colon);
                buffer.put(headers.getString(key).getBytes());
                buffer.put(vert);
            }
            buffer.put(space);
            KeyValue properties = writeMsg.properties();
            if(properties != null){
                for(String key: properties.keySet()) {
                    buffer.put(key.getBytes());
                    buffer.put(colon);
                    buffer.put(properties.getString(key).getBytes());
                    buffer.put(vert);
                }
            }
            buffer.put(space);
            buffer.put(writeMsg.getBody());
            buffer.put(br);
        }
        //logger.info(" bucket:" + bucket + " slice:" + slice + " Size: "+ len + " current Filename: " + WriteToDisk.fileIndex);
        if(buffer.remaining() < 2*1024*1024) {
            //logger.info("start fill buffer->" +buffer+ buffer.remaining());
            buffer.flip();
            synchronized (fullBufferList) {
                // reget Buffer from queue
                fullBufferList.add(buffer);
                fullBufferList.notifyAll();
            }
        } else {
            synchronized (availBufferList) {
                availBufferList.add(buffer);
                availBufferList.notifyAll();
            }
        }
    }

    public  Message buildMessage(String line){
        int index = line.indexOf(' ');
        int lastIndex = line.lastIndexOf(' ');
        String header = line.substring(0, index);

        String properties = line.substring(index + 1, lastIndex);
        String body = line.substring(lastIndex + 1);

        BytesMessage message = new DefaultBytesMessage(body.getBytes());
        String info;
        while((index = header.indexOf('|')) > 0){
            info = header.substring(0, index);
            int split = info.indexOf(':');
            if(split > 0) {
                message.headers().put(info.substring(0, split), info.substring(split + 1));
                header = header.substring(index + 1);
            } else {
                break;
            }
        }
        while((index = properties.indexOf('|')) > 0){
            info = properties.substring(0, index);
            int split = info.indexOf(':');
            if(split > 0) {
                message.putProperties(info.substring(0, split), info.substring(split + 1));
                properties = properties.substring(index + 1);
            } else {
                break;
            }
        }
        return message;
    }

    public Message pullMessage(String queue, String bucket) {
        Message message;
        int currentSlice = 0;
        ConcurrentHashMap<String, Integer> queueBucketSlice = null;

        if (currentRead.get(queue) == null) {
            queueBucketSlice = new ConcurrentHashMap<>();
            currentRead.put(queue, queueBucketSlice);
            currentSlice = 0;
        } else {
            queueBucketSlice = currentRead.get(queue);
            currentSlice = queueBucketSlice.getOrDefault(bucket, 0);
        }

        FileSlice readSlice = new FileSlice(bucket, currentSlice);
        ArrayList<String> bucketList = rawBuckets.get(readSlice);
        if(bucketList == null) {
            return null;
        }
        ConcurrentHashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new ConcurrentHashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);

        // reach the end of the file.
        if (offset >= bucketList.size()) {
            synchronized (remainReadConsumer) {
                int remainThread = remainReadConsumer.get(readSlice);
                if (remainThread > 1)
                    remainReadConsumer.put(readSlice, remainThread-1);
                else {
                    synchronized (lockerForReadControl) {
                        rawBuckets.remove(readSlice);
                        lockerForReadControl.notifyAll();
                    }
                }
            }
            readSlice = new FileSlice(bucket, ++currentSlice);
            offsetMap.put(bucket, 0);
            offset = 0;

            queueBucketSlice.put(bucket, currentSlice);
            bucketList = rawBuckets.get(readSlice);

            if(bucketList == null) {
                return null;
            }
        }
        message = buildMessage(bucketList.get(offset));
        offsetMap.put(bucket, ++offset);

        return message;
   }

    public  void flush(){
        long start = System.currentTimeMillis();
        Set buckets = messageBuckets.keySet();
        Iterator iterator = buckets.iterator();
        while(iterator.hasNext()) {
            String bucket = (String) iterator.next();
            ArrayList<Message> al = messageBuckets.get(bucket);

            if(al.size() != 0) {
                int slice = fileSplit.getOrDefault(bucket, 0);;
                ByteBuffer currentByteBuffer = null;
                synchronized (availBufferList) {

                    while (availBufferList.size() == 0) {
                        try {
                            availBufferList.wait();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    currentByteBuffer = availBufferList.getFirst();
                    availBufferList.remove(currentByteBuffer);
                }
                mergeMessage(bucket, slice, al, currentByteBuffer);
            }
            al =null;
        }
        int len = availBufferList.size();
        for(int i = 0; i < len; i++) {
            ByteBuffer remain = availBufferList.get(i);

            if(remain.position() > 0) {
                remain.flip();
                synchronized (fullBufferList) {
                    fullBufferList.add(remain);
                    fullBufferList.notifyAll();
                }
            }
        }
        WriteToDisk.stopWrite();
        synchronized (fullBufferList) {
            fullBufferList.notifyAll();
        }
        //clear
        messageBuckets.clear();
        producerLock.clear();

        //logger.info("set flag to stop...  flush Message count ->" + sum);
        //logger.info("Total write File Count:" + WriteToDisk.fileIndex +"");
        try{
            WriteToDisk.getWriteThread().join();
        } catch(Exception e) {
            e.printStackTrace();
        }
        logger.info("end flush... use time: " + (System.currentTimeMillis() - start));

    }
}

