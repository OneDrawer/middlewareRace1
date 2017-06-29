package io.openmessaging.tester;

import io.openmessaging.demo.FileSlice;
import io.openmessaging.demo.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.ImagingOpException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by mst on 2017/5/14.
 */
public class MainTest {

    public static Logger logger = LoggerFactory.getLogger(MainTest.class);
    public static ByteBuffer bb = ByteBuffer.allocate(35 * 1024 * 1024);
    public static void main(String[] args) throws Exception{
       loadFile(10000);

    }
    public static boolean loadFile(int fileIndex) throws Exception {

            for (int index = 10000; index <= 11185; index++) {
                File load = new File(new StringBuffer("result").append("/").append(index).toString());

                if (!load.exists()) {

                    break;
                }

                //logger.info("load " + index);
                FileReader fr = new FileReader(load);
                BufferedReader br = new BufferedReader(fr);

                String topicOrQueue;
                String slice;
                FileSlice fs = null;
                String line = null;
                bb.put((index + "").getBytes());
                bb.put("\n".getBytes());
                while ((line = br.readLine()) != null) {
                    if (line.equals("$")) {
                        bb.put((index + " ").getBytes());
                        bb.put(" ".getBytes());
                        topicOrQueue = br.readLine();
                        slice = br.readLine();
                        bb.put(topicOrQueue.getBytes());
                        bb.put(":".getBytes());
                        bb.put(slice.getBytes());
                        bb.put("\n".getBytes());
                    }
                }
                //rawBuckets.put(fileSlice, msgList);

            }
        FileOutputStream fos =new FileOutputStream(new File("1.txt"));
        FileChannel fch = fos.getChannel();
        bb.flip();
        fch.write(bb);


        File file = new File("2.txt");
        logger.info(file.getAbsolutePath());
        file.createNewFile();
        FileOutputStream fos2 = new FileOutputStream(file);
        fos2.write("dsadsad".getBytes());
        fos2.close();


        fch.close();
        fos.close();
        return true;
        }
    }