package com.dsp.utils;

import com.dsp.aws.SQSclient;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GeneralUtils {

    public static final String VISIBILITY = "10";

    public static String toBase64(String data) {
        String base64UserData = null;
        base64UserData = new String(Base64.getEncoder().encode(data.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        return base64UserData;
    }

    public static String getUniqueID() {
        Long l = new Random().nextLong();
        return Long.toHexString(l);
    }


    public static String initSqs(String queueName, SQSclient sqs) {
        try {
            return sqs.getQueueUrl(queueName);
        } catch(Exception e){
            if(!sqs.createQueue(queueName, VISIBILITY)){
                System.out.println("Error at creating localToManagerQueue");
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
