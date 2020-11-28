package com.dsp.utils;

import com.dsp.aws.SQSClient;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GeneralUtils {

    public static final String VISIBILITY = "30";
    public static final String LOG_FILE = "jars/debug.log";

    public GeneralUtils(){
        try {
            if(!new File(LOG_FILE).createNewFile()){
                System.out.println("In GeneralUtils: can't create file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String toBase64(String data) {
        return new String(Base64.getEncoder().encode(data.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    public static String getUniqueID() {
        long l = new Random().nextLong();
        return Long.toHexString(l);
    }

    public synchronized void logPrint(String msg){
        String s = String.format("%s - %s", new Date().toString(), msg);
        System.out.println(s);
        String logString = s + "\n";
        try {
            Files.write(Paths.get(LOG_FILE), logString.getBytes(), StandardOpenOption.APPEND);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String initSqs(String queueName, SQSClient sqs) {
        try {
            return sqs.getQueueUrl(queueName);
        } catch(Exception e){
            if(!sqs.createQueue(queueName, VISIBILITY)){
                System.out.println("Error at creating queue " + queueName);
                System.exit(1);
            }
            try{
                TimeUnit.SECONDS.sleep(2);
            } catch(Exception ex){
                e.printStackTrace();
                System.exit(1);
            }
            return sqs.getQueueUrl(queueName);
        }
    }
}
