package com.dsp.manager;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSclient;
import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Manager {

    public static final String MANAGER_TO_WORKERS_QUEUE_NAME = "managerToWorkersQueue_"+ GeneralUtils.getUniqueID();
    public static final String WORKERS_TO_MANAGER_QUEUE_NAME = "workersToManagerQueue_"+ GeneralUtils.getUniqueID();
    public static final Integer MAX_INSTANCES = 18; // max instances of student aws account is 19
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSclient sqs;
    private static String localToManagerQueueUrl;
    private static String managerToLocalQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workersToManagerQueueUrl;
    private static Integer numOfActiveWorkers;
    private static Integer sizeOfCurrentInput;
    private static String arn;
    private static String ami;
    private static String s3BucketName;
    private static String keyName;
    private static Map<String, Map<String, String>> tasksResults;

    public static void main(String[] args) {
        System.out.println("started manager process");

        int n = Integer.parseInt(args[0]);
        String localToManagerQueueName = args[1];
        String managerToLocalQueueName = args[2];
        s3BucketName = args[3];
        ami = args[4];
        arn = args[5];
        keyName = args[6];

        //get the queue URL's for the local app
        localToManagerQueueUrl = sqs.getQueueUrl(localToManagerQueueName);
        managerToLocalQueueUrl = sqs.getQueueUrl(managerToLocalQueueName);

        //create the queues for the workers
        managerToWorkersQueueUrl = GeneralUtils.initSqs(MANAGER_TO_WORKERS_QUEUE_NAME, sqs);
        workersToManagerQueueUrl = GeneralUtils.initSqs(WORKERS_TO_MANAGER_QUEUE_NAME, sqs);

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSclient();

        AtomicBoolean shouldContinue = new AtomicBoolean(true);
        numOfActiveWorkers = 0;
        sizeOfCurrentInput = 0;

        ExecutorService executor = Executors.newFixedThreadPool(5);
        executor.execute(() -> {
            while (!Thread.interrupted()) {
                List<Message> messages = sqs.getMessages(localToManagerQueueUrl, 1);
                handleMessage(n, shouldContinue, executor, messages);
            }
        });

        while (shouldContinue.get()){
            //poll queue for results
        }

        //terminate seq kill all ec2 and sqs


    }

    private static void handleMessage(int n, AtomicBoolean shouldContinue, ExecutorService executor, List<Message> messages) {
        if(!messages.isEmpty()){
            Message message = messages.get(0);
            String body = message.body();
            if(body.equals("terminate")){
                shouldContinue.compareAndSet(true,false);
                executor.shutdown();
                if(!sqs.deleteMessages(messages, localToManagerQueueUrl)){
                    System.out.println("Error at deleting task message from localToManagerQueue");
                    System.exit(1); // Fatal Error
                }
            }
            else{
                distributeTasks(n, messages, body);
            }
        }
    }

    //TODO heavy task, check if VISIBILTY is enough
    private static void distributeTasks(int n, List<Message> messages, String body) {
        String inputFilePath = body +"_input.txt";
        if(!s3.getObject(s3BucketName, body, inputFilePath)) { // body is the key in s3
            System.out.println("Error downloading input file from s3");
            return;
        }
        List<String> urlList = parseInputFile(inputFilePath);
        synchronized (sizeOfCurrentInput){
            sizeOfCurrentInput+=urlList.size();
        }
        loadBalance(n);
        sendTasks(body, urlList);
        if(!sqs.deleteMessages(messages, localToManagerQueueUrl)){
            System.out.println("Error at deleting task message from localToManagerQueue");
            System.exit(1); // Fatal Error
        }
    }

    private static void sendTasks(String LocalAppID, List<String> urlList) {
        Map<String, String> subTasksResult = new HashMap<>();
        for (String url: urlList) {
            subTasksResult.put(url, "");
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
            attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(LocalAppID).build());
            if(!sqs.sendMessage(managerToWorkersQueueUrl, url, attributesMap)) {
                System.out.println("Error at sending task message to worker");
                System.exit(1); // Fatal Error
            }
        }
        tasksResults.put(LocalAppID, subTasksResult);
    }

    // checks if there are enough workers running, if not creates them.
    private static void loadBalance(int n) {
        synchronized (sizeOfCurrentInput){
            int numOfWorkersNeeded = sizeOfCurrentInput % n == 0 ? sizeOfCurrentInput / n : (sizeOfCurrentInput/n)+1;
            numOfWorkersNeeded = Math.min(numOfWorkersNeeded, MAX_INSTANCES);
            if(numOfWorkersNeeded <= numOfActiveWorkers){
                return;
            }
            int delta = numOfWorkersNeeded - numOfActiveWorkers;
            String userData = createWorkerScript();
            List<Instance> instances = ec2.createEC2Instances(ami, keyName, delta, delta, userData, arn, InstanceType.T2_MICRO);
            if(instances != null){
                numOfActiveWorkers = numOfWorkersNeeded;
            }
        }
    }

    private static List<String> parseInputFile(String inputFilePath) {
        try {
            return Files.readAllLines(Paths.get(inputFilePath), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    private static String createWorkerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo mkdir yarintry";
        return GeneralUtils.toBase64(userData);
    }

}
