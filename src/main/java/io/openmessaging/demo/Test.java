package io.openmessaging.demo;

import java.io.*;

import io.openmessaging.tester.Constants;
/**
 * Created by xwz on 6/3/17.
 */
public class Test {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int fileIndex = 10000;
        while (true) {
            if (!loadFile(fileIndex++)) {
                break;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public static boolean loadFile(int fileIndex) {
        try {
            File load = new File(new StringBuffer(Constants.STORE_PATH).append("/").append(fileIndex).toString());
            if (!load.exists()) {
                return false;
                //loadCompleted = true;
            }
            FileReader fr = new FileReader(load);
            BufferedReader br = new BufferedReader(fr);
            String line;
            String tmpLine = null;
            while((line = br.readLine()) != null) {
                if(tmpLine == line)
                    ;

                if(tmpLine == line + "1")
                    ;

                if(tmpLine == line + "2")
                    ;
                if(tmpLine == line + "3")
                    ;
                if(tmpLine == line + "4")
                    ;
                tmpLine = line;
            }
            //System.out.println(line);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //return true;
        }
        return true;
    }
}
