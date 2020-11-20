package com.dsp.application;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSclient;
import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

//ec2 role: ec2roledsp1
//yarin - aws arn: arn:aws:iam::322970830450:instance-profile/ec2_role_full_access
// omer - aws arn: arn:aws:iam::290318388667:instance-profile/ec2_role_full_access

public class LocalApplication {
    public static final String VISIBILITY = "10";
    private static boolean isManagerDone = false;
    private static boolean shouldTerminate = false;
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSclient sqs;
    private static LocalAppConfiguration config;
    private static String s3BucketName = null;
    private static String localToManagerQueueUrl = null;
    private static String responseKey = null;

    public static void main( String[] args){

        if(args.length < 3) {
            System.out.println("Error: At least 3 arguments needed - inputFileName, outputFileName, n");
            System.exit(1);
        }

        //cli args
        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);
        if(args.length == 4 && args[3].equals("terminate")) {
            shouldTerminate = true;
        }

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSclient();

        //init configuration object
        config = new LocalAppConfiguration();

        //check if manager node is up, if not we will start it and all relevant aws services
        initManager();

        //upload input file to s3 + send message to manager
        String s3InputFileKey = "task_"+ GeneralUtils.getUniqueID();
        //if upload to s3 was successful, sends message to manager
        sendTask(inputFileName, s3InputFileKey);

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
        if(s3.getObject(s3BucketName, responseKey, outputFileName+"_temp")){
            createHtml();
        }
        else{
            System.out.println("Error at downloading summary file from s3 bucket");
            System.exit(1);
        }

        //if received shouldTerminate in args, send terminate message to manager TODO maybe change to while()
        if(shouldTerminate){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            if(!sqs.sendMessage(localToManagerQueueUrl,"terminate",attributesMap)) {
                System.out.println("Error at sending terminate message to manager");
                //System.exit(1);
            }
        }
        System.out.println("exiting");
    }

    private static void sendTask(String inputFileName, String s3InputFileKey) {
        //upload input file to s3 bucket
        if(s3.putObject(s3BucketName,s3InputFileKey,inputFileName)){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            if(!sqs.sendMessage(localToManagerQueueUrl, s3InputFileKey,attributesMap)) {
                System.out.println("Error at sending task message to manager");
                System.exit(1);
            }
        }
        else{
            System.out.println("Error at uploading input file to s3 bucket");
            System.exit(1);
        }
    }

    //create final html output file
    private static void createHtml() {
        //TODO

    }

    //check if manager finished task (message in managerToLocalQueue)
    private static boolean checkResponse() {
        //TODO

        return false;
    }

    //check if manager node is up, if not we will start it and all aws services required
    private static void initManager() {
        Filter filter = Filter.builder()
                .name("instance-state-name")
                .values("running")
                .build();
        List<Instance> runningInstances = ec2.getAllInstances(filter);
        //find the running instances
        for (Instance instance : runningInstances) {
            for (Tag tag : instance.tags()) {
                if (tag.value().equals("manager")) {
                    return;
                }
            }
        }

        //check s3 and sqs services (bucket and 2 queues) are created, if not we create them
        initServices();
        //manager node is not running
        s3.putObject(config.getS3BucketName(), "jars/manager.jar", "./jars/manager.jar");
        System.out.println("uploading worker jar file...");
        s3.putObject(config.getS3BucketName(), "jars/worker.jar", "./jars/worker.jar");
        List<Instance> instances = ec2.createEC2Instances(config.getAmi(), config.getAwsKeyPair(), 1, 1, createManagerScript(), config.getArn(), config.getInstanceType());
        String instanceId = instances.get(0).instanceId();
        ec2.createTag("Name", "manager", instanceId);
    }

    //init AWS services: sqs and s3 (only if needed)
    private static void initServices() {
        //init for s3 bucket
        s3BucketName = config.getS3BucketName();
        if(!s3.getAllBucketNames().contains(s3BucketName)){
            s3.createBucket(s3BucketName);
        }
        //init for all sqs queues
        initSqs(config.getLocalToManagerQueueName());
        initSqs(config.getManagerToLocalQueueName());
    }

    private static void initSqs(String localToManagerQueueName) {
        try {
            localToManagerQueueUrl = sqs.getQueueUrl(localToManagerQueueName);
        } catch(Exception e){
            if(!sqs.createQueue(localToManagerQueueName, VISIBILITY)){
                System.out.println("Error at creating localToManagerQueue");
                System.exit(1);
            }
            try{
                TimeUnit.SECONDS.sleep(2);
            } catch(Exception ex){
                e.printStackTrace();
                System.exit(1);
            }
            localToManagerQueueUrl = sqs.getQueueUrl(localToManagerQueueName);
        }
    }

    private static String createManagerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo mkdir yarintry";
        return GeneralUtils.toBase64(userData);
    }

    private static void ec2Test(String arn) {
        EC2Client ec2Client = new EC2Client();
        String amiId = "ami-076515f20540e6e0b";
        InstanceType instanceType = InstanceType.T2_MICRO;
        List<Instance> instances = ec2Client.createEC2Instances(amiId, "ec2key_dsp1", 1, 1, createManagerScript(), arn, instanceType);
    }

}
