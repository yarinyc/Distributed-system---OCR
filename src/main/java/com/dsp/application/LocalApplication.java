package com.dsp.application;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSclient;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;


//ec2 role: ec2roledsp1
//yarin - aws arn: arn:aws:iam::322970830450:instance-profile/ec2_role_full_access

// omer - aws arn: arn:aws:iam::290318388667:instance-profile/ec2_role_full_access

public class LocalApplication
{
    private static boolean isManagerRunning = false;
    private static boolean isManagerDone = false;
    private static boolean shouldTerminate = false;
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSclient sqs;
    private static String s3BucketName = null;
    private static String localToManagerQueueUrl = null;
    private static String managerToLocalQueueUrl = null;
    private static String responseKey = null;

    public static void main( String[] args){

        if(args.length < 3) {
            System.out.println("Error: At least 3 arguments needed - inputFileName, outputFileName, n");
            System.exit(1);
        }

        //cli args
        String inputFileName = args[0];
        String outputFileName = args[1];
        String n = args[2];
        if(args.length == 4)
            if(args[3].equals("terminate"))
                shouldTerminate = true;

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSclient();

        //check s3 and sqs services (bucket and 2 queues) are created, if not we create them
        checkServices();

        //check if manager node is up, if not we start it and set isManagerRunning field to be true
        checkInitManager();

        //upload input file to s3 + send message to manager
        String fileLocation = "task"+System.currentTimeMillis();
        //if upload to s3 was successful, send message to manager
        if(uploadInputFile(inputFileName,fileLocation)){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            if(!sqs.sendMessage(localToManagerQueueUrl,fileLocation,attributesMap)) {
                System.out.println("Error at sending task message to manager");
                System.exit(1);
            }
        }
        else{
            System.out.println("Error at uploading input file to s3 bucket");
            System.exit(1);
        }

        //poll managerToLocal queue for response
        while(!isManagerDone){
            //once there is a response, the responseKey field will be different from null
            //we will send it as an attribute in the response message from the manager and change
            //the responseKey field value in checkResponse
            try {
                TimeUnit.SECONDS.sleep(20);
            } catch (Exception e){
                e.printStackTrace();
                System.exit(1);
            }
            isManagerDone = checkResponse();
        }

        //get summary file from s3 bucket and create output html file
        if(s3.getObject(s3BucketName, responseKey, "out.txt")){
            createHtml();
        }
        else{
            System.out.println("Error at downloading summary file from s3 bucket");
            System.exit(1);
        }

        //if received shouldTerminate in args, send terminate message to manager
        if(shouldTerminate){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            if(!sqs.sendMessage(localToManagerQueueUrl,"terminate",attributesMap)) {
                System.out.println("Error at sending terminate message to manager");
                //System.exit(1);
            }
        }

    }

    //create final html output file
    private static void createHtml() {
    }

    //check if manager finished task (message in managerToLocalQueue)
    private static boolean checkResponse() {
        return false;
    }

    //check AWS services existence
    private static void checkServices() {
        if(s3BucketName == null){
            s3.createBucket("locals_manager_shared_bucket");
            s3BucketName = "locals_manager_shared_bucket";
        }

        if(localToManagerQueueUrl == null){
            sqs.createQueue("localToManager","10");
            //sleep to make sure queue is created
            try{
            TimeUnit.SECONDS.sleep(2);
            } catch(Exception e){
                e.printStackTrace();
                System.exit(1);
            }
            //set url field value
            localToManagerQueueUrl = sqs.getQueueUrl("localToManager");
        }

        if(managerToLocalQueueUrl == null){
            sqs.createQueue("ManagerToLocal","10");
            //sleep to make sure queue is created
            try{
                TimeUnit.SECONDS.sleep(2);
            } catch(Exception e){
                e.printStackTrace();
            }
            //set url field value
            managerToLocalQueueUrl = sqs.getQueueUrl("ManagerToLocal");
        }
    }

    // upload input file to s3 bucket, return if succeeded or not
    private static boolean uploadInputFile(String inputFile, String fileKey) {
        return s3.putObject(s3BucketName,fileKey,inputFile);
    }

    //check if manager node is up, if not we start it
    private static void checkInitManager() {
    }



    private static void ec2Test(String arn) {
        EC2Client ec2Client = new EC2Client();
        String amiId = "ami-076515f20540e6e0b";
        InstanceType instanceType = InstanceType.T2_MICRO;
        List<Instance> instances = ec2Client.createEC2Instances(amiId, "ec2key_dsp1", 1, 1, createManagerScript(), arn, instanceType);
    }

    private static void s3Test(){
        S3client client = new S3client();
        client.createBucket("aaabbbcccguru");
        client.putObject("aaabbbcccguru", "omerkey", "C:\\Users\\ycohen\\Desktop\\omerfile.txt");
        client.getObject("aaabbbcccguru", "omerkey", "C:\\Users\\ycohen\\Desktop\\out.txt");
        System.out.println("done");
    }


    private static void sqsTest() {
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

    private static String createManagerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo mkdir yarintry";
        return toBase64(userData);
    }

    private static String toBase64(String data) {
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(data.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64UserData;
    }
}
