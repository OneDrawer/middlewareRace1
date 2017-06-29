package io.openmessaging.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.demo.MessageStore.*;

/**
 * Created by mst on 2017/5/23.
 */
class WriteToDisk extends Thread{

    private static Thread INSTANCE = new WriteToDisk();
    public  static volatile boolean shouldStop = false;
    public volatile static int fileIndex  = 10000;
    static Logger logger = LoggerFactory.getLogger(ReadDisk.class);
    public static Thread getWriteThread(){
        return INSTANCE;
    }
    private WriteToDisk(){}
    public static void loadThreadToStart(){}

    //run when first loaded
    static{
        INSTANCE.setPriority(MAX_PRIORITY);
        INSTANCE.setName("mst");
        INSTANCE.start();
    }
    public static void stopWrite(){
        shouldStop = true;
    }
    @Override
    public void run() {
        //CompressList.loadCompressThread();
        while (true) {
            ByteBuffer currentWriteBuffer;
            synchronized (fullBufferList) {
                while(fullBufferList.size() == 0) {
                    try {
                        if(shouldStop)
                            break;
                        fullBufferList.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if(shouldStop && fullBufferList.size() == 0) {
                    for(int i = 0; i < BUFFER_SIZE; i++) {
                        DirectBuffer db = (DirectBuffer) availBufferList.get(i);
                        db.cleaner().clear();
                    }
                    availBufferList.clear();
                    //logger.info("Produce has finished, Confirm information!");
                    //logger.info("Write End... Count =" + fileIndex);
                    break;
                }
                currentWriteBuffer = fullBufferList.getFirst();
                fullBufferList.remove(currentWriteBuffer);

            }

            writeFile(fileIndex++, currentWriteBuffer);
            currentWriteBuffer.clear();
            synchronized (availBufferList) {
                availBufferList.add(currentWriteBuffer);
                availBufferList.notifyAll();
            }

        }
    }
    public void writeFile(int fileIndex, ByteBuffer currentWrite) {
        try {
            StringBuilder filename = new StringBuilder(STORE_PATH).append("/").append(fileIndex);
            FileOutputStream fos = new FileOutputStream(new File(filename.toString()));
            FileChannel fch = fos.getChannel();
            fch.write(currentWrite);
            currentWrite.clear();
            fch.close();
            fos.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
class ReadDisk extends Thread {
    private static Thread INSTANCE = new ReadDisk();
    public static volatile boolean loadCompleted = false;
    private static String STORE_PATH = MessageStore.STORE_PATH;
    public static int fileIndex = 10000;
    static Logger logger = LoggerFactory.getLogger(ReadDisk.class);
    public Map<FileSlice, ArrayList<String>> rawBuckets = MessageStore.getInstance().getRawBucket();
    public static ConcurrentHashMap<String, Integer> maxConsumer = MessageStore.getInstance().numOfConsumer;

    private ReadDisk() {
    }
    // initialize read thread
    public static void loadThreadToStart() {
    }
    //run when first loaded
    static {
        INSTANCE.setPriority(MAX_PRIORITY);
        INSTANCE.setName("mst");
        INSTANCE.start();
    }

    @Override
    public void run() {
        while (true) {
            if (!loadFile(fileIndex++)) {
                break;
            }
        }
    }
    public boolean loadFile(int fileIndex) {
        try {
            File load = new File(new StringBuffer(STORE_PATH).append("/").append(fileIndex).toString());

            if (!load.exists()) {
                loadCompleted = true;
                return false;
            }
            synchronized (lockerForReadControl){
                //logger.info("load and raw Bucket Size = " + rawBuckets.size());
                while(rawBuckets.size() > 3000) {
                    try {
                        lockerForReadControl.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            FileReader fr = new FileReader(load);
            BufferedReader br = new BufferedReader(fr);
            //if no consumer for this topic!
            ArrayList<String> msgList = null;
            String topicOrQueue;
            int slice;
            FileSlice fs = null;
            String line = null;
            int len;
            while ((line = br.readLine()) != null) {
                topicOrQueue = line;
                slice = Integer.parseInt(br.readLine());
                len = Integer.parseInt(br.readLine());
                int count = maxConsumer.getOrDefault(topicOrQueue, 0);
                if(count == 0) {
                    int i = 0;
                    while(i++<len){
                        br.readLine();
                    }
                    continue;
                }
                fs = new FileSlice(topicOrQueue, slice);
                msgList = new ArrayList<>(len);
                for(int i = 0; i < len; i++) {
                    msgList.add(br.readLine());
                }
                remainReadConsumer.put(fs, maxConsumer.getOrDefault(fs.bucket, 0));
                rawBuckets.put(fs, msgList);
            }
            //rawBuckets.put(fileSlice, msgList);
            logger.info("load File --> " + fileIndex + " Load slice ");
            br.close();
            fr.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        return true;
    }
}