package com.dsp.manager;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSClient;
import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.core.util.json.JacksonUtils;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Manager {

    public static final String MANAGER_TO_WORKERS_QUEUE_NAME = "managerToWorkersQueue_"+ GeneralUtils.getUniqueID();
    public static final String WORKERS_TO_MANAGER_QUEUE_NAME = "workersToManagerQueue_"+ GeneralUtils.getUniqueID();
    public static final Integer MAX_INSTANCES = 17; // max instances of student aws account is 19
    private static final int NUM_OF_THREADS = Math.max(Runtime.getRuntime().availableProcessors(), 8);
    private static final String instanceId = EC2MetadataUtils.getInstanceId();

    private static EC2Client ec2;
    private static S3client s3;
    private static SQSClient sqs;
    private static GeneralUtils generalUtils;

    private static ExecutorService resultExecutor;

    private static String localToManagerQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workersToManagerQueueUrl;

    //hashmap of hashmaps: Outer hashmap: key=localAppID, value=Inner hashmap: key=url of task, value=url counters
    private static Map<String, Map<String, AtomicInteger>> urlCounters;

    private static Map<String, Map<String, String>> uniqueIDToUrlMap;
    //hashmap in which key is LocalAppID and value is a counter of completed subtasks
    private static Map<String, AtomicInteger> completedSubTasksCounters;
    //hashmap for storing managerToLocal queues for all different local apps
    private static Map<String, String> managerToLocalQueues;
    //hashmap for all the input sizes of each localAppID
    private static Map<String, Integer> localAppInputSizes;
    //hashmap for the number of workers needed per local app
    private static Map<String, Integer> workersNeededPerApp;

    //number of running worker nodes
    private static Integer numOfActiveWorkers;
    //number of total subtasks in the system
//    private static Integer sizeOfCurrentInput;
    private static final Object lock = new Object();
    //config fields
    private static String arn;
    private static String ami;
    private static String s3BucketName;
    private static String keyName;
    private static boolean shouldDeleteS3;

    public static void main(String[] args) {

        //get data from args
        String localToManagerQueueName = args[0];
        s3BucketName = args[1];
        ami = args[2];
        arn = args[3];
        keyName = args[4];
        shouldDeleteS3 = Boolean.parseBoolean(args[5]);

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
        uniqueIDToUrlMap = new ConcurrentHashMap<>();
        workersNeededPerApp = new ConcurrentHashMap<>();
        completedSubTasksCounters = new ConcurrentHashMap<>();
        urlCounters = new ConcurrentHashMap<>();
        managerToLocalQueues = new ConcurrentHashMap<>();
        localAppInputSizes = new ConcurrentHashMap<>();

        AtomicInteger shutdownCounter = new AtomicInteger(0);
        AtomicBoolean shouldRun = new AtomicBoolean(true);

        resultExecutor = Executors.newFixedThreadPool(4);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_THREADS);
        //start all localToManagerQueue listeners
        for(int i=0; i<NUM_OF_THREADS; i++) {
            executor.submit(() -> {
                while (shouldRun.get()){
                    List<Message> messages = sqs.getMessages(localToManagerQueueUrl, 1);
                    try {
                        handleMessage(messages, shouldRun);
                    } catch (Exception e){
                        GeneralUtils.printStackTrace(e, generalUtils);
                        generalUtils.logPrint("Error in listener thread: handleMessage failed, continuing...");
                    }
                }
                int count = shutdownCounter.incrementAndGet(); // signal the main thread that this thread is finished
                generalUtils.logPrint("ShutdownCounter is " + count);
            });
        }

        // check periodically that there are enough workers running
        ExecutorService loadBalanceExecutor = Executors.newFixedThreadPool(1);
        loadBalanceExecutor.submit(()-> {
            while(shouldRun.get()) {
                checkWorkerBalance();
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {

                    GeneralUtils.printStackTrace(e, generalUtils);
                }
            }
        });

        while (shutdownCounter.get() != NUM_OF_THREADS || !completedSubTasksCounters.isEmpty()){
            //poll queue for results
            try {
                List<Message> messages = sqs.getMessages(workersToManagerQueueUrl, 5);
                for (Message m : messages) {
                    //get all needed information and the result
                    generalUtils.logPrint("Handling result message");
                    handleResultMessage(m);
                }
                if(messages.isEmpty()){
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException e) {
                        GeneralUtils.printStackTrace(e, generalUtils);
                    }
                }
            } catch (Exception e){
                GeneralUtils.printStackTrace(e, generalUtils);
                generalUtils.logPrint("Error in listener thread: handleMessage failed, continuing...");
            }
        }
        generalUtils.logPrint("Calling termination sequence");
        terminateSequence();
        // END OF MAIN
    }

    private static void checkWorkerBalance() {
        int delta;
        List<Instance> instances;
        int numOfWorkersNeeded = 0;
        try {
            numOfWorkersNeeded = Collections.max(workersNeededPerApp.values());
        } catch (NoSuchElementException e) {
            GeneralUtils.printStackTrace(e,generalUtils);
        }
        Filter filter = Filter.builder()
                .name("instance-state-name")
                .values("running", "pending")
                .build();
        synchronized (lock) {
            numOfActiveWorkers = ec2.getNumberOfWorkerInstances(filter);

            if (numOfWorkersNeeded <= numOfActiveWorkers) {
                generalUtils.logPrint("In loadBalance: No extra workers needed. currently #" + numOfActiveWorkers);
                return;
            }
            delta = numOfWorkersNeeded - numOfActiveWorkers;
            generalUtils.logPrint("In loadBalance: " + delta + " more workers needed. currently (before) #" + numOfActiveWorkers);
            String userData = createWorkerScript();
            instances = ec2.createEC2Instances(ami, keyName, delta, delta, userData, arn, InstanceType.T2_MICRO);
        }
        if (instances != null) {
            for (Instance instance : instances) {
                if (!ec2.createTag("Name", "worker", instance.instanceId())) {
                    generalUtils.logPrint("Error in manager: loadBalance ec2.createTag with instance Id: " + instance.instanceId());
                }
            }
        }
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

        //increment the counter for this url subtask
        urlCounters.get(localAppID).get(url).incrementAndGet();

        String uid = GeneralUtils.getUniqueID();
        uniqueIDToUrlMap.get(localAppID).put(url, uid);
        if(!s3.putObjectFromMemory(s3BucketName,localAppID+"_result/results/"+uid, result)){
            generalUtils.logPrint("Error in putting url result to s3: " + url);
        }
        int new_count = completedSubTasksCounters.get(localAppID).incrementAndGet();

        //check if now all subtasks of localAppID are done
        if(new_count == localAppInputSizes.get(localAppID)){
            generalUtils.logPrint("Completing task for local app ID: " + localAppID);
            completedSubTasksCounters.remove(localAppID); //delete counter, task is done
            localAppInputSizes.remove(localAppID);
            workersNeededPerApp.remove(localAppID);
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

    //Create summary file and send to the local application
    //The summary file consists 2 hashmaps translated to json format:
    //A counter hashmap that holds a counter for every url subtask (for taking care of duplicates)
    //A hashmap that translates uniqueId values to urls. We will use this hashmap upon creating the html file
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
        //convert the two hashmaps to json format
        String counterJson = JacksonUtils.toJsonString(urlCounters.get(localAppID));
        String uidJson = JacksonUtils.toJsonString(uniqueIDToUrlMap.get(localAppID));
        //write them to the file
        try {
            out.write(counterJson+"\n"+uidJson);
            out.flush();
            out.close();
        } catch (IOException e) {
            GeneralUtils.printStackTrace(e,generalUtils);
            generalUtils.logPrint("Error in createSendSummaryFile: out.write(jsonResult)" );
        }
        //save file to the s3 bucket
        String responseKey = localAppID + "_result";
        if(!s3.putObject(s3BucketName, responseKey, localAppID+"_result.txt")){
            generalUtils.logPrint("Error in createSendSummaryFile: s3.putObject");
        }
        //send message to the relevant local app
        String queueUrl = managerToLocalQueues.get(localAppID);
        HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
        attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
        attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("LocalApp").build());
        if(!sqs.sendMessage(queueUrl, responseKey, attributesMap)){
            generalUtils.logPrint("Error in createSendSummaryFile: sqs.sendMessage");
            System.exit(1); //Fatal Error
        }
        urlCounters.remove(localAppID); //delete url counters map
        managerToLocalQueues.remove(localAppID); //delete queue url from map
        uniqueIDToUrlMap.remove(localAppID); //delete uniqueIdToUrl map
        //delete temporary file from memory
        if(!new File(localAppID+"_result.txt").delete()){
            generalUtils.logPrint("Error in createSendSummaryFile: summary file deletion");
        }
    }

    private static void handleMessage(List<Message> messages,AtomicBoolean shouldRun) {
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
            }
            else{
                //add manager to local app queue to map
                String queueUrl = message.messageAttributes().get("managerToLocalQueueUrl").stringValue();
                managerToLocalQueues.put(body, queueUrl);
                //break up task to subtasks and send to workers
                int n = Integer.parseInt(message.messageAttributes().get("N").stringValue());

                distributeTasks(n, messages, body); // body is the localAppID
            }
        }
        else{
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                GeneralUtils.printStackTrace(e, generalUtils);
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

        generalUtils.logPrint("Distributing " + urlList.size() + " subtasks to workers queue");

        //check there is a sufficient number of workers
        loadBalance(n, urlList.size(), localAppID);

        //send url tasks to workers
        sendTasks(localAppID, urlList);
        //delete task message from queue (we just sent all subtasks to the workers)
        if(!sqs.deleteMessages(messages, localToManagerQueueUrl)){
            generalUtils.logPrint("Error at deleting task message from localToManagerQueue");
            System.exit(1); // Fatal Error
        }
    }

    //sends url tasks to workers
    private static void sendTasks(String localAppID, List<String> urlList) {
        Map<String, AtomicInteger> subTasksCounters = new ConcurrentHashMap<>();
        Map<String, String> uidToUrlMap = new ConcurrentHashMap<>();
        completedSubTasksCounters.put(localAppID, new AtomicInteger(0)); //so far there are 0 completed subtasks(urls) of localAppID
        localAppInputSizes.put(localAppID, urlList.size());
        for (String url: urlList) {
            subTasksCounters.put(url, new AtomicInteger(0));
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
            attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(localAppID).build());
            if(!sqs.sendMessage(managerToWorkersQueueUrl, url, attributesMap)) {
                generalUtils.logPrint("Error at sending task message to worker");
                System.exit(1); // Fatal Error
            }
        }
        urlCounters.put(localAppID, subTasksCounters); // we add a new results hashmap of LocalAppID
        uniqueIDToUrlMap.put(localAppID, uidToUrlMap);
    }

    //checks if there are enough workers running, if not creates them
    private static void loadBalance(int n, int sizeOfCurrentInput, String localAppID) {
        int numOfWorkersNeeded;
        int delta;
        List<Instance> instances;
        numOfWorkersNeeded = sizeOfCurrentInput % n == 0 ? sizeOfCurrentInput / n : (sizeOfCurrentInput / n) + 1;
        numOfWorkersNeeded = Math.min(numOfWorkersNeeded, MAX_INSTANCES);
        workersNeededPerApp.put(localAppID, numOfWorkersNeeded);

        Filter filter = Filter.builder()
                .name("instance-state-name")
                .values("running","pending")
                .build();
        synchronized (lock) {
            numOfActiveWorkers = ec2.getNumberOfWorkerInstances(filter);

            if(numOfWorkersNeeded <= numOfActiveWorkers){
                generalUtils.logPrint("In loadBalance: No extra workers needed. currently #" + numOfActiveWorkers );
                return;
            }
            delta = numOfWorkersNeeded - numOfActiveWorkers;
            generalUtils.logPrint("In loadBalance: " + delta + " more workers needed. currently (before) #" + numOfActiveWorkers );
            String userData = createWorkerScript();
            instances = ec2.createEC2Instances(ami, keyName, delta, delta, userData, arn, InstanceType.T2_MICRO);
        }
        if(instances != null){
            for (Instance instance : instances) {
                if(!ec2.createTag("Name", "worker", instance.instanceId())){
                    generalUtils.logPrint("Error in manager: loadBalance ec2.createTag with instance Id: " + instance.instanceId());
                }
            }
        }
        generalUtils.logPrint("In loadBalance: " + delta + " more workers needed. currently (after) #" + numOfWorkersNeeded );
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
        //kill manager node
        ec2.terminateInstances(Stream.of(instanceId).collect(Collectors.toList()));
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
                .values("running", "pending")
                .build();
        List<Instance> runningInstances = ec2.getAllInstances(filter);
        //find the running instances
        runningInstances = runningInstances.stream().filter( instance -> {
            for (Tag tag : instance.tags()) {
                if (tag.value().equals("worker")) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());

        //kill all running instances
        if(runningInstances.isEmpty() || !ec2.terminateInstances(runningInstances.stream().map(Instance::instanceId).collect(Collectors.toList()))){
            generalUtils.logPrint("No worker instances were terminated");
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
        userData += String.format("sudo java -jar /jars/worker.jar %s %s", managerToWorkersQueueUrl, workersToManagerQueueUrl);

        return GeneralUtils.toBase64(userData);
    }

}
