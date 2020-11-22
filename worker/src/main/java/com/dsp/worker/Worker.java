package com.dsp.worker;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import com.dsp.aws.SQSclient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker {
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSclient sqs;

    public static void main(String[] args) {

        //get queue urls from args
        String managerToWorkersQueueUrl = args[0];
        String workersToManagerQueueUrl = args[1];

        //boolean flag for running
        boolean shouldTerminate = false;

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSclient();

        //create OCR engine
        Tesseract tesseract = new Tesseract();

        while(!shouldTerminate){
            List<Message> messages = sqs.getMessages(managerToWorkersQueueUrl, 5);
            for(Message m : messages){
                if(m.body().equals("terminate")){
                    shouldTerminate = true;
                }
                else{
                    handleOcrTask(m,managerToWorkersQueueUrl);
                }
            }
        }
    }

    //handle OCR Task
    private static void handleOcrTask(Message m, String managerToWorkersQueueUrl) {
        Map<String, MessageAttributeValue> attributes = m.messageAttributes();
        String localAppID = attributes.get("LocalAppID").stringValue();
        String inputUrl = m.body();
        //download image
        String imagePath = downloadImage(inputUrl);
        if (imagePath.equals("")) {
            System.out.println("Error: Image not downloaded.... continuing to next ocr task, URL: " +  inputUrl);
            //send worker exception notification to manager
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(localAppID).build());
            attributesMap.put("Url", MessageAttributeValue.builder().dataType("String").stringValue(inputUrl).build());
            attributesMap.put("ExceptionSummary", MessageAttributeValue.builder().dataType("String").stringValue("Image download error").build());
            if(!sqs.sendMessage(managerToWorkersQueueUrl, "WORKER EXCEPTION", attributesMap)) {
                System.out.println("Error at sending worker exception to manager");
                System.exit(1); // Fatal Error
            }
        }
        //apply ocr on the image
        String ocrResult = applyOcr(imagePath, tesseract);
        if(ocrResult == null){
            System.out.println("Error during OCR operation.... continuing to next ocr task, URL: "+ inputUrl);
            //send worker exception notification to manager
            HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
            attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
            attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
            attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(localAppID).build());
            attributesMap.put("Url", MessageAttributeValue.builder().dataType("String").stringValue(inputUrl).build());
            attributesMap.put("ExceptionSummary", MessageAttributeValue.builder().dataType("String").stringValue("OCR operation error").build());
            if(!sqs.sendMessage(managerToWorkersQueueUrl, "WORKER EXCEPTION", attributesMap)) {
                System.out.println("Error at sending worker exception to manager");
                System.exit(1); // Fatal Error
            }
        }
        //send ocr result to manager
        HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
        attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
        attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
        attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(localAppID).build());
        attributesMap.put("Url", MessageAttributeValue.builder().dataType("String").stringValue(inputUrl).build());
        if(!sqs.sendMessage(managerToWorkersQueueUrl, ocrResult, attributesMap)) {
            System.out.println("Error at sending OCR task result to manager, URL: " + inputUrl);
            System.exit(1); // Fatal Error
        }

        //delete ocr task message from queue - only if OCR was successful!
        List<Message> msgToDelete = new ArrayList<>();
        msgToDelete.add(m);
        if(!sqs.deleteMessages(msgToDelete, managerToWorkersQueueUrl)){
            System.out.println("Error at deleting task message from managerToWorkersQueue");
            System.exit(1); // Fatal Error
        }
    }

    private static String applyOcr(String imagePath, Tesseract tesseract){
        try {
            //TODO: check if setDataPath should be the path of the downloaded image
            tesseract.setDatapath("./");
            // apply OCR on the image
            return tesseract.doOCR(new File(imagePath));
        }
        catch (TesseractException e) {
            e.printStackTrace();
            return null;
        }
    }

    //downloads image from url
    private static String downloadImage(String urlInput) {
        String downloadFilePath = urlInput + "_Image.jpg";
        try{
            URL url = new URL(urlInput);
            InputStream inputStream = url.openStream();
            OutputStream fileOutputStream = new FileOutputStream(downloadFilePath);
            int ch;
            while ((ch = inputStream.read()) != -1) { //read till end of file
                fileOutputStream.write(ch);
            }
            inputStream.close();
            fileOutputStream.close();
            return downloadFilePath;
        }
        catch (IOException e){
            System.out.println("Error at creating url object");
            return "";
        }
    }


}
