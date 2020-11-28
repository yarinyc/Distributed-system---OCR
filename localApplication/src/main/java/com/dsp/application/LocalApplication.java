package com.dsp.application;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSClient;
import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LocalApplication {
    private static boolean isManagerDone = false;
    private static boolean shouldTerminate = false;
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSClient sqs;
    private static  GeneralUtils generalUtils;
    private static LocalAppConfiguration config;
    private static String s3BucketName = null;
    private static String localToManagerQueueUrl = null;
    private static String managerToLocalQueueUrl = null;
    private static String responseKey = null;
    private static int n;

    public static void main(String[] args){

        generalUtils = new GeneralUtils();
        generalUtils.logPrint("started LocalApplication");
        System.exit(1);

        if(args.length < 3) {
            generalUtils.logPrint("Error: At least 3 arguments needed - inputFileName, outputFileName, n");
            System.exit(1);
        }

        //cli args
        String inputFileName = args[0];
        String outputFileName = args[1];
        n = Integer.parseInt(args[2]);
        if(args.length == 4 && args[3].equals("terminate")) {
            shouldTerminate = true;
        }

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSClient();

        //init configuration object
        config = new LocalAppConfiguration();

        s3BucketName = config.getS3BucketName();

        //check if manager node is up, if not we will start it and all relevant aws services
        generalUtils.logPrint("Initializing AWS services...");
        initManager();
        generalUtils.logPrint("Done Initializing AWS services");

        if(localToManagerQueueUrl == null){
            localToManagerQueueUrl = sqs.getQueueUrl(config.getLocalToManagerQueueName());
        }

        //upload input file to s3 + send message to manager
        String localAppID = GeneralUtils.getUniqueID();

        // init an sqs queue for manager to local communication
        managerToLocalQueueUrl = GeneralUtils.initSqs("managerToLocalQueueUrl_"+localAppID, sqs);

        //if upload to s3 was successful, sends message to manager
        sendTask(inputFileName, localAppID);

        //poll managerToLocal queue for response
        System.out.print("Waiting for response");
        while(!isManagerDone){
            //once there is a response, the responseKey field will be different from null
            //we will send it as an attribute in the response message from the manager and change
            //the responseKey field value in checkResponse
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e){
                e.printStackTrace();
                System.exit(1);
            }
            isManagerDone = checkResponse();
            System.out.print(".");
        }

        generalUtils.logPrint("\nReceiving response from manager");

        //get summary file from s3 bucket and create output html file
        if(s3.getObject(s3BucketName, responseKey, outputFileName+"_temp")){
            createHtml(outputFileName);
        }
        else{
            generalUtils.logPrint("Error at downloading summary file from s3 bucket");
            System.exit(1);
        }

        //if received shouldTerminate in args, send terminate message to manager
        if(shouldTerminate){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            if(!sqs.sendMessage(localToManagerQueueUrl,"terminate",attributesMap)) {
                generalUtils.logPrint("Error at sending terminate message to manager");
                //System.exit(1);
            }
        }

        if(!sqs.deleteQueue(managerToLocalQueueUrl)){
            generalUtils.logPrint("Error at deleting sqs queue managerToLocalQueueUrl");
        }

        generalUtils.logPrint("Exiting local application");
    }

    private static void sendTask(String inputFileName, String s3InputFileKey) {
        //upload input file to s3 bucket
        if(s3.putObject(s3BucketName,s3InputFileKey,inputFileName)){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("managerToLocalQueueUrl", MessageAttributeValue.builder().dataType("String").stringValue(managerToLocalQueueUrl).build());
            if(!sqs.sendMessage(localToManagerQueueUrl, s3InputFileKey,attributesMap)) {
                generalUtils.logPrint("Error at sending task message to manager");
                System.exit(1);
            }
        }
        else{
            generalUtils.logPrint("Error at uploading input file to s3 bucket");
            System.exit(1);
        }
    }

    //create final html output file
    private static void createHtml(String outputFileName) {
//        if(!s3.getObject(s3BucketName, responseKey, outputFileName+"_temp")){
//            generalUtils.logPrint("Error in createHtml");
//        }
        generalUtils.logPrint("creating HTML to " + outputFileName);
        //TODO create the actual html

    }

    //check if manager finished task (message in managerToLocalQueue)
    private static boolean checkResponse() {
        List<Message> messages = sqs.getMessages(managerToLocalQueueUrl, 1);
        for (Message m : messages) {
            responseKey = m.body();
            return true;
        }
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

        //short sleep to make sure s3 is ready with the jars
//        try {
//            TimeUnit.SECONDS.sleep(4);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        List<Instance> instances = ec2.createEC2Instances(config.getAmi(), config.getAwsKeyPair(), 1, 1, createManagerScript(), config.getArn(), config.getInstanceType());
        String instanceId = instances.get(0).instanceId();
        ec2.createTag("Name", "manager", instanceId);
    }

    //init AWS services: sqs and s3 (only if needed)
    private static void initServices() {
        //init for s3 bucket
        if(!s3.getAllBucketNames().contains(s3BucketName)){
            generalUtils.logPrint("Creating S3 bucket");

            if(!s3.createBucket(s3BucketName)){
                generalUtils.logPrint("Error in local app: s3.createBucket");
                System.exit(1);
            }

            generalUtils.logPrint("uploading manager jar file...");
            if(!s3.putObject(config.getS3BucketName(), "jars/manager.jar", Paths.get("jars","manager.jar").toString())){
                generalUtils.logPrint("Error in local app: s3.putObject manager.jar");
                System.exit(1);
            }

            generalUtils.logPrint("uploading worker jar file...");
            if(!s3.putObject(config.getS3BucketName(), "jars/worker.jar", Paths.get("jars","worker.jar").toString())){
                generalUtils.logPrint("Error in local app: s3.putObject worker.jar");
                System.exit(1);
            }
        }
        //init local to manager sqs queue
        localToManagerQueueUrl = GeneralUtils.initSqs(config.getLocalToManagerQueueName(), sqs);
    }

    private static String createManagerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo mkdir /jars/\n";
        userData = userData + "sudo aws s3 cp s3://" + s3BucketName + "/jars/manager.jar /jars/\n";
        userData += String.format("sudo java -jar /jars/manager.jar %s %s %s %s %s %s",
                n, config.getLocalToManagerQueueName(), config.getS3BucketName(),
                config.getAmi(), config.getArn(), config.getAwsKeyPair());

        return GeneralUtils.toBase64(userData);
    }

}
