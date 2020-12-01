package com.dsp.manager;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSClient;
import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.core.util.json.JacksonUtils;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Manager {

    public static final String MANAGER_TO_WORKERS_QUEUE_NAME = "managerToWorkersQueue_"+ GeneralUtils.getUniqueID();
    public static final String WORKERS_TO_MANAGER_QUEUE_NAME = "workersToManagerQueue_"+ GeneralUtils.getUniqueID();
    public static final Integer MAX_INSTANCES = 18; // max instances of student aws account is 19
    private static final int NUM_OF_THREADS = 8;

    private static EC2Client ec2;
    private static S3client s3;
    private static SQSClient sqs;
    private static GeneralUtils generalUtils;

    private static ExecutorService executor;
    private static ExecutorService resultExecutor;

    private static String localToManagerQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workersToManagerQueueUrl;

    //hashmap of hashmaps: Outer hashmap: key=localAppID, value=Inner hashmap: key=url of task, value=result of url
    //each localApp has it's own hashmap (Inner hashmap) of task results
    private static Map<String, Map<String, String>> tasksResults;
    //hashmap in which key is LocalAppID and value is a counter of completed subtasks
    private static Map<String, AtomicInteger> completedSubTasksCounters;
    private static Map<String, String> managerToLocalQueues;

    private static Integer numOfActiveWorkers;
    private static Integer sizeOfCurrentInput;
    private static final Object lock = new Object();

    private static String arn;
    private static String ami;
    private static String s3BucketName;
    private static String keyName;
    private static boolean shouldDeleteS3;


    public static void main(String[] args) {

        //get data from args
        int n = Integer.parseInt(args[0]);
        String localToManagerQueueName = args[1];
        s3BucketName = args[2];
        ami = args[3];
        arn = args[4];
        keyName = args[5];
        shouldDeleteS3 = Boolean.parseBoolean(args[6]);

        //init AWS clients
        generalUtils = new GeneralUtils();
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSClient();

        generalUtils.logPrint("Started manager process");

        //get the queue URL's for the local app
        localToManagerQueueUrl = sqs.getQueueUrl(localToManagerQueueName);

        //create the queues for the workers
        managerToWorkersQueueUrl = GeneralUtils.initSqs(MANAGER_TO_WORKERS_QUEUE_NAME, sqs);
        workersToManagerQueueUrl = GeneralUtils.initSqs(WORKERS_TO_MANAGER_QUEUE_NAME, sqs);

        numOfActiveWorkers = 0;
        sizeOfCurrentInput = 0;
        completedSubTasksCounters = new ConcurrentHashMap<>();
        tasksResults = new ConcurrentHashMap<>();
        managerToLocalQueues = new ConcurrentHashMap<>();

        AtomicInteger shutdownCounter = new AtomicInteger(0);
        AtomicBoolean shouldRun = new AtomicBoolean(true);

        resultExecutor = Executors.newFixedThreadPool(4);
        executor = Executors.newFixedThreadPool(NUM_OF_THREADS);
        //start all localToManagerQueue listeners
        for(int i=0; i<NUM_OF_THREADS; i++) {
            executor.submit(() -> {
                while (shouldRun.get()){
                    List<Message> messages = sqs.getMessages(localToManagerQueueUrl, 1);
                    handleMessage(n, messages, shouldRun);
                }
                int count = shutdownCounter.incrementAndGet(); // signal the main thread that this thread is finished
                generalUtils.logPrint("ShutdownCounter is " + count);
            });
        }

        while (shutdownCounter.get() != NUM_OF_THREADS || !completedSubTasksCounters.isEmpty()){
            //poll queue for results
            List<Message> messages = sqs.getMessages(workersToManagerQueueUrl, 5);
            for(Message m : messages){
                //get all needed information and the result
                generalUtils.logPrint("Handling result message");
                handleResultMessage(m);
            }
        }
        generalUtils.logPrint("Calling termination sequence");
        terminateSequence();
    }

    private static void terminateSequence() {
        //delete s3 bucket
        if(shouldDeleteS3){
            s3.deleteBucket(s3BucketName);
        }

        //delete all existing sqs queues
        terminateSqs();

        // kill all running ec2 instances
        terminateEc2();
    }

    private static void terminateSqs() {
        if(!sqs.deleteQueue(localToManagerQueueUrl)){
            generalUtils.logPrint("Error: localToManagerQueue couldn't be deleted");
        }
        if(!sqs.deleteQueue(managerToWorkersQueueUrl)){
            generalUtils.logPrint("Error: managerToWorkersQueue couldn't be deleted");
        }
        if(!sqs.deleteQueue(workersToManagerQueueUrl)){
            generalUtils.logPrint("Error: workersToManagerQueue couldn't be deleted");
        }
        //send all waiting clients a manager terminated message
        for (String queueUrl : managerToLocalQueues.values()) {
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
            if(!sqs.sendMessage(queueUrl, "MANAGER_TERMINATED", attributesMap)) {
                generalUtils.logPrint("Error at sending task message to local app");
            }
        }
    }

    private static void terminateEc2() {
        Filter filter = Filter.builder()
                .name("instance-state-name")
                .values("running")
                .build();
        List<Instance> runningInstances = ec2.getAllInstances(filter);
        //find the running instances
        List<Instance> managerInstance = new ArrayList<>();
        runningInstances = runningInstances.stream().filter( instance -> {
            for (Tag tag : instance.tags()) {
                if (tag.value().equals("worker")) {
                    return true;
                }
                if (tag.value().equals("manager")) {
                    managerInstance.add(instance);
                    return false;
                }
            }
            return false;
        }).collect(Collectors.toList());

        //kill all running instances
        if(runningInstances.isEmpty() || !ec2.terminateInstances(runningInstances.stream().map(Instance::instanceId).collect(Collectors.toList()))){
            generalUtils.logPrint("No worker instances were terminated");
        }

        //kill manager node
        ec2.terminateInstances(managerInstance.stream().map(Instance::instanceId).collect(Collectors.toList()));
    }

    private static void handleResultMessage(Message m) {
        Map<String, MessageAttributeValue> attributes = m.messageAttributes();
        String localAppID = attributes.get("LocalAppID").stringValue();
        String url = attributes.get("Url").stringValue();
        String result = m.body();

        //check if an exception occurred in worker node
        if(result.equals("WORKER EXCEPTION")){
            result = attributes.get("ExceptionSummary").stringValue();
            generalUtils.logPrint("Received WORKER EXCEPTION");
        }

        //add result to hashmap + update counter of completed tasks of localAppID
        tasksResults.get(localAppID).put(url, result);
        int new_count = completedSubTasksCounters.get(localAppID).incrementAndGet();
        //completedSubTasksCounters.put(localAppID,new_count);

        //check if now all subtasks of localAppID are done
        if(new_count == tasksResults.get(localAppID).size()){
            generalUtils.logPrint("Completing task for local app ID: " + localAppID);
            synchronized (lock){
                sizeOfCurrentInput -= tasksResults.get(localAppID).size();
            }
            completedSubTasksCounters.remove(localAppID); //delete counter, task is done
            generalUtils.logPrint("Submitting task result to resultExecutor" + localAppID);

            resultExecutor.submit(()-> createSendSummaryFile(localAppID));
        }

        //delete message from queue
        List<Message> msgToDelete = new ArrayList<>();
        msgToDelete.add(m);
        generalUtils.logPrint("Deleting message from workersToManagerQueueUrl");
        if(!sqs.deleteMessages(msgToDelete, workersToManagerQueueUrl)){
            generalUtils.logPrint("Error at deleting task message from workersToManagerQueue");
            System.exit(1); // Fatal Error
        }
    }

    //Create summary file of all url subtasks results in json format and send to the local application
    private static void createSendSummaryFile(String localAppID) {
        FileWriter fStream;
        try {
            fStream = new FileWriter(localAppID+"_result.txt");
        } catch (IOException e) {
            GeneralUtils.printStackTrace(e,generalUtils);
            generalUtils.logPrint("Error in createSendSummaryFile: FileWriter(localAppID+\"_result.txt\")");
            return;
        }
        BufferedWriter out = new BufferedWriter(fStream);

        String jsonResult = JacksonUtils.toJsonString(tasksResults.get(localAppID));
        generalUtils.logPrint("JSON STRING: " + jsonResult);
        try {
            out.write(jsonResult);
            out.flush();
            out.close();
        } catch (IOException e) {
            GeneralUtils.printStackTrace(e,generalUtils);
            generalUtils.logPrint("Error in createSendSummaryFile: out.write(jsonResult)" );
        }

        String responseKey = localAppID + "_result";
        if(!s3.putObject(s3BucketName, responseKey, localAppID+"_result.txt")){
            generalUtils.logPrint("Error in createSendSummaryFile: s3.putObject");
        }
        String queueUrl = managerToLocalQueues.get(localAppID);
        HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
        attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
        attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
        if(!sqs.sendMessage(queueUrl, responseKey, attributesMap)){
            generalUtils.logPrint("Error in createSendSummaryFile: sqs.sendMessage");
            System.exit(1); // Fatal Error
        }
        tasksResults.remove(localAppID); // delete sub tasks map
        managerToLocalQueues.remove(localAppID); // delete queue url from map
        if(!new File(localAppID+"_result.txt").delete()){
            generalUtils.logPrint("Error in createSendSummaryFile: summary file deletion");
        }
    }

    private static void handleMessage(int n, List<Message> messages,AtomicBoolean shouldRun) {
        if(!messages.isEmpty()){
            Message message = messages.get(0);
            String body = message.body();
            if(body.equals("terminate")){
                generalUtils.logPrint("Terminate message received in manager");
                if(!sqs.deleteMessages(messages, localToManagerQueueUrl)){
                    generalUtils.logPrint("Error at deleting task message from localToManagerQueue");
                    System.exit(1); // Fatal Error
                }
                shouldRun.set(false);
//                executor.shutdown();
            }
            else{
                //add manager to local app queue to map
                String queueUrl = message.messageAttributes().get("managerToLocalQueueUrl").stringValue();
                managerToLocalQueues.put(body, queueUrl);
                //break up task to subtasks and send to workers
                distributeTasks(n, messages, body); // body is the localAppID
            }
        }
    }

    private static void distributeTasks(int n, List<Message> messages, String localAppID) {
        String inputFilePath = localAppID +"_input.txt";
        if(!s3.getObject(s3BucketName, localAppID, inputFilePath)) { // body is the key in s3
            generalUtils.logPrint("Error downloading input file from s3");
            return;
        }
        List<String> urlList = parseInputFile(inputFilePath);
        //filter any unwanted strings
        urlList = urlList.stream().filter(url-> !(url.equals("") || url.equals("\n"))).collect(Collectors.toList());
        synchronized (lock){
            sizeOfCurrentInput+=urlList.size();
        }

        generalUtils.logPrint("Distributing " + sizeOfCurrentInput + "subtasks to workers queue");

        //check there is a sufficient number of workers
        loadBalance(n);
        //send url tasks to workers
        sendTasks(localAppID, urlList);
        //delete task message from queue (we just sent all subtasks to the workers)
        if(!sqs.deleteMessages(messages, localToManagerQueueUrl)){  //TODO heavy task, maybe move deletion to before this function
            generalUtils.logPrint("Error at deleting task message from localToManagerQueue");
            System.exit(1); // Fatal Error
        }
    }

    //sends url tasks to workers
    private static void sendTasks(String LocalAppID, List<String> urlList) {
        Map<String, String> subTasksResult = new HashMap<>();
        completedSubTasksCounters.put(LocalAppID, new AtomicInteger(0)); //so far there are 0 completed subtasks(urls) of localAppID
        for (String url: urlList) {
            subTasksResult.put(url, "####default-value####");
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
            attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(LocalAppID).build());
            if(!sqs.sendMessage(managerToWorkersQueueUrl, url, attributesMap)) {
                generalUtils.logPrint("Error at sending task message to worker");
                System.exit(1); // Fatal Error
            }
        }
        tasksResults.put(LocalAppID, subTasksResult); // we add a new results hashmap of LocalAppID
    }

    // checks if there are enough workers running, if not creates them.
    private static void loadBalance(int n) {
        synchronized (lock){
            int numOfWorkersNeeded = sizeOfCurrentInput % n == 0 ? sizeOfCurrentInput / n : (sizeOfCurrentInput/n)+1;
            numOfWorkersNeeded = Math.max(numOfWorkersNeeded, 1); // in case (n > inputSize)
            numOfWorkersNeeded = Math.min(numOfWorkersNeeded, MAX_INSTANCES);
            if(numOfWorkersNeeded <= numOfActiveWorkers){
                return;
            }
            int delta = numOfWorkersNeeded - numOfActiveWorkers;
            String userData = createWorkerScript();
            List<Instance> instances = ec2.createEC2Instances(ami, keyName, delta, delta, userData, arn, InstanceType.T2_MICRO);
            if(instances != null){
                numOfActiveWorkers = numOfWorkersNeeded;
                for (Instance instance : instances) {
                    if(!ec2.createTag("Name", "worker", instance.instanceId())){
                        generalUtils.logPrint("Error in manager: loadBalance ec2.createTag with instance Id: " + instance.instanceId());
                    }
                }
            }
        }
    }

    //auxiliary function for reading input files
    private static List<String> parseInputFile(String inputFilePath) {
        try {
            return Files.readAllLines(Paths.get(inputFilePath), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            generalUtils.logPrint(Arrays.toString(e.getStackTrace()));
            return Collections.emptyList();
        }
    }

    private static String createWorkerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo mkdir /jars/\n";
        userData = userData + "sudo aws s3 cp s3://" + s3BucketName + "/jars/worker.jar /jars/\n";
        userData += String.format("sudo java -jar /jars/worker.jar %s %s", managerToWorkersQueueUrl, workersToManagerQueueUrl);

        return GeneralUtils.toBase64(userData);
    }

}
