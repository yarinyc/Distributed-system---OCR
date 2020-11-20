package com.dsp.application;

import static org.junit.Assert.assertTrue;

import com.dsp.aws.S3client;
import com.dsp.aws.SQSclient;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for simple App.
 */
public class LocalApplicationTest
{

    @Test
    private void s3Test(){
        S3client client = new S3client();
        client.createBucket("aaabbbcccguru");
        client.putObject("aaabbbcccguru", "omerkey", "C:\\Users\\ycohen\\Desktop\\omerfile.txt");
        client.getObject("aaabbbcccguru", "omerkey", "C:\\Users\\ycohen\\Desktop\\out.txt");
        System.out.println("done");
    }

    @Test
    private void sqsTest() {
        SQSclient sqs = new SQSclient();
        boolean isCreated = sqs.createQueue("queue","5");
        System.out.println(isCreated);

        String url = sqs.getQueueUrl("queue");

        String message1 = "Message 1";
        HashMap<String, MessageAttributeValue> attributesMap1 = new HashMap<>();
        attributesMap1.put("Owner", MessageAttributeValue.builder().dataType("String").stringValue("omer").build());
        boolean isSent1 = sqs.sendMessage(url,message1,attributesMap1);
        System.out.println(isSent1);

        String message2 = "Message 2";
        HashMap<String, MessageAttributeValue> attributesMap2 = new HashMap<>();
        attributesMap2.put("Owner", MessageAttributeValue.builder().dataType("String").stringValue("omer").build());
        boolean isSent2 = sqs.sendMessage(url,message2,attributesMap2);
        System.out.println(isSent2);

        String message3 = "Message 3";
        HashMap<String, MessageAttributeValue> attributesMap3 = new HashMap<>();
        attributesMap2.put("Owner", MessageAttributeValue.builder().dataType("String").stringValue("omer").build());
        boolean isSent3 = sqs.sendMessage(url,message3,attributesMap3);
        System.out.println(isSent3);

        String message4 = "Message 4";
        HashMap<String, MessageAttributeValue> attributesMap4 = new HashMap<>();
        attributesMap2.put("Owner", MessageAttributeValue.builder().dataType("String").stringValue("omer").build());
        boolean isSent4 = sqs.sendMessage(url,message4,attributesMap4);
        System.out.println(isSent4);

        String message5 = "Message 5";
        HashMap<String, MessageAttributeValue> attributesMap5 = new HashMap<>();
        attributesMap2.put("Owner", MessageAttributeValue.builder().dataType("String").stringValue("omer").build());
        boolean isSent5 = sqs.sendMessage(url,message5,attributesMap5);
        System.out.println(isSent5);

        try {
            System.out.println("going to sleep");
            TimeUnit.SECONDS.sleep(7);
        }   catch (Exception e) {
            e.printStackTrace();
        }

        List<Message> readMessages = sqs.getMessages(url,5);
        while(readMessages.size() > 0){
            for(Message m : readMessages){
                System.out.println(m.body());
            }
            sqs.deleteMessages(readMessages,url);
            readMessages = sqs.getMessages(url,5);
        }

        sqs.deleteQueue(url);
    }
}
