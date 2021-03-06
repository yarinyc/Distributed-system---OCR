package com.dsp.application;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSClient;
import com.dsp.utils.GeneralUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalApplication {
    private static final String DELETE_S3 = "true";
    private static boolean isManagerDone = false;
    private static boolean shouldTerminate = false;
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSClient sqs;
    private static GeneralUtils generalUtils;
    private static LocalAppConfiguration config;
    private static String s3BucketName = null;
    private static String localToManagerQueueUrl = null;
    private static String managerToLocalQueueUrl = null;
    private static String responseKey = null;
    private static int n;
    private static ObjectMapper mapper;

    public static void main(String[] args){

        //init generalUtils object
        generalUtils = new GeneralUtils();

        generalUtils.logPrint("Local application started");

        //mapper object to convert jackson file to java hashmap
        mapper = new ObjectMapper();

        if(args.length < 3) {
            generalUtils.logPrint("Error: At least 3 arguments needed - inputFileName, outputFileName, n");
            System.exit(1);
        }

        //cli args
        String inputFileName = args[0];
        String outputFileName = args[1];
        n = Math.max(Integer.parseInt(args[2]), 1);
        if(args.length == 4 && args[3].equals("terminate")) {
            shouldTerminate = true;
        }

        //init configuration object
        config = new LocalAppConfiguration();

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSClient();

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
                TimeUnit.SECONDS.sleep(2);
            } catch (Exception e){
                generalUtils.logPrint(Arrays.toString(e.getStackTrace()));
                System.exit(1);
            }
            isManagerDone = checkResponse();
            System.out.print(".");
        }
        System.out.println();

        generalUtils.logPrint("Receiving response from manager");

        if(responseKey.equals("MANAGER_TERMINATED")){
            generalUtils.logPrint("Exiting local application: manager node terminated early...");
            if(!sqs.deleteQueue(managerToLocalQueueUrl)){
                generalUtils.logPrint("Error at deleting sqs queue managerToLocalQueueUrl");
            }
            System.exit(0);
        }

        //tempId for temp result file
        String tempId = GeneralUtils.getUniqueID();

        //get summary file from s3 bucket and create output html file
        if(s3.getObject(s3BucketName, responseKey, "temps"+ File.separator +outputFileName+"_"+tempId)){
            createHtml(outputFileName,tempId);
        }
        else{
            generalUtils.logPrint("Error at downloading summary file from s3 bucket");
        }

        terminateSequence(localAppID);

        generalUtils.logPrint("Exiting local application");
    }

    private static void sendTask(String inputFileName, String s3InputFileKey) {
        //upload input file to s3 bucket
        if(s3.putObject(s3BucketName,s3InputFileKey,inputFileName)){
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("N", MessageAttributeValue.builder().dataType("String").stringValue(Integer.toString(n)).build());
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
    private static void createHtml(String outputFileName, String tempId) {
        generalUtils.logPrint("creating HTML to " + outputFileName);
        int writeThreshold = 0;
        try {
            List<String> mapJsonString = Files.readAllLines(Paths.get("temps", outputFileName+"_"+tempId), StandardCharsets.UTF_8);

            File outputFile = new File("outputs"+ File.separator + outputFileName +".html");
            if(!outputFile.createNewFile()){
                generalUtils.logPrint("Error while creating output file");
            }
            String prefix = "<html>\n" + "<title>OCR</title>\n" + "<body>\n";
            appendToFile("outputs"+ File.separator + outputFileName +".html",prefix);

            //convert JSON string to Map
            TypeFactory typeFactory = mapper.getTypeFactory();
            MapType mapType1 = typeFactory.constructMapType(HashMap.class, String.class, AtomicInteger.class);
            HashMap<String, AtomicInteger> resultCounters = mapper.readValue(mapJsonString.get(0), mapType1);

            MapType mapType2 = typeFactory.constructMapType(HashMap.class, String.class, String.class);
            HashMap<String, String> uidToUrl = mapper.readValue(mapJsonString.get(1), mapType2);

            //build html string
            StringBuilder ocrResults = new StringBuilder();

            for(HashMap.Entry<String, String> entry : uidToUrl.entrySet()) {
                String url = entry.getKey();
                String uid = entry.getValue();
                String result = s3.getObjectToMemory(s3BucketName, responseKey+"/results/"+uid);
                AtomicInteger counter = resultCounters.get(url);
                for(int i=0; i<counter.get(); i++) {
                    ocrResults.append("\t<p>\n" + "\t\t<img src=\"")
                            .append(url).append("\"><br/>\n")
                            .append("\t\t")
                            .append(result.replaceAll("\n", "<br/>"))
                            .append("\n")
                            .append("\t</p>\n");
                    writeThreshold++;
                    if(writeThreshold == 100){
                        appendToFile("outputs"+ File.separator + outputFileName +".html",ocrResults.toString());
                        writeThreshold = 0;
                        ocrResults = new StringBuilder();
                    }
                }

            }
            if(!ocrResults.toString().isEmpty()){
                appendToFile("outputs"+ File.separator + outputFileName +".html",ocrResults.toString());
            }
            String suffix = "</body>\n" + "<html>";
            appendToFile("outputs"+ File.separator + outputFileName +".html",suffix);
            if(!new File(Paths.get("temps", outputFileName +"_"+ tempId).toString()).delete()){
                generalUtils.logPrint("temp image can't be deleted");
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error at creating html file");
        }
    }



    private static void appendToFile(String outputFilePath, String toWrite) {
        try {
            Files.write(Paths.get(outputFilePath), toWrite.getBytes(), StandardOpenOption.APPEND);
        }catch (IOException e) {
            e.printStackTrace();
            generalUtils.logPrint("Error in createHtml: appendToFile");
        }
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
                .values("running", "pending")
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

         /* generalUtils.logPrint("uploading manager jar file...");
            if(!s3.putObject(config.getS3BucketName(), "jars/manager.jar", Paths.get("./jars","manager.jar").toString())){
                generalUtils.logPrint("Error in local app: s3.putObject manager.jar");
                System.exit(1);
            }

            generalUtils.logPrint("uploading worker jar file...");
            if(!s3.putObject(config.getS3BucketName(), "jars/worker.jar", Paths.get("./jars","worker.jar").toString())){
                generalUtils.logPrint("Error in local app: s3.putObject worker.jar");
                System.exit(1);
            } */
        }
        //init local to manager sqs queue
        generalUtils.logPrint("creating localToManagerQueue");
        localToManagerQueueUrl = GeneralUtils.initSqs(config.getLocalToManagerQueueName(), sqs);
    }

    private static String createManagerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData += String.format("sudo java -jar /jars/manager.jar %s %s %s %s %s %s",
                config.getLocalToManagerQueueName(), config.getS3BucketName(),
                config.getAmi(), config.getArn(), config.getAwsKeyPair(), DELETE_S3);

        return GeneralUtils.toBase64(userData);
    }

    private static void terminateSequence(String localAppID) {
        //if received shouldTerminate in args, send terminate message to manager
        if(shouldTerminate){
            generalUtils.logPrint("Terminating manager node");
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            if(!sqs.sendMessage(localToManagerQueueUrl,"terminate",attributesMap)) {
                generalUtils.logPrint("Error at sending terminate message to manager");
            }
        }

        if(!sqs.deleteQueue(managerToLocalQueueUrl)){
            generalUtils.logPrint("Error at deleting sqs queue managerToLocalQueueUrl");
        }

        if(!s3.deleteObject(s3BucketName, localAppID) | !s3.deleteObject(s3BucketName, responseKey) ){
            generalUtils.logPrint("Error in deleting S3 objects");
        }

        List<String> resultKeys = s3.getAllObjectsKeys(s3BucketName, responseKey+"/results");
        boolean deletedAll = true;
        for(String key : resultKeys){
            deletedAll = deletedAll & s3.deleteObject(s3BucketName, key);
        }
        if (!deletedAll){
            generalUtils.logPrint("Error in deleting results from S3");
        }

    }

}
