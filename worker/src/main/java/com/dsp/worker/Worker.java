package com.dsp.worker;

import com.dsp.utils.GeneralUtils;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import java.awt.*;
import java.awt.image.BufferedImage;
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


import javax.imageio.ImageIO;
import java.util.*;
import java.util.List;

public class Worker {
    private static EC2Client ec2;
    private static S3client s3;
    private static SQSclient sqs;
    private static Tesseract tesseract;

    public static void main(String[] args) {

        //get queue urls from args
        String managerToWorkersQueueUrl = args[0];
        String workersToManagerQueueUrl = args[1];

        //init AWS clients
        ec2 = new EC2Client();
        s3 = new S3client();
        sqs = new SQSclient();

        //create OCR engine
        tesseract = new Tesseract();
        // /usr/share/tesseract-ocr/
        tesseract.setDatapath("C:\\Users\\ycohen\\Desktop\\tessdata");

        while(!Thread.interrupted()){
            List<Message> messages = sqs.getMessages(managerToWorkersQueueUrl, 1);
            for(Message m : messages){
                handleOcrTask(m,workersToManagerQueueUrl, managerToWorkersQueueUrl);
            }
        }
        System.out.println("Worker finished");
    }

    //handle OCR Task
    private static void handleOcrTask(Message m, String workersToManagerQueueUrl, String managerToWorkersQueueUrl) {
        Map<String, MessageAttributeValue> attributes = m.messageAttributes();
        String localAppID = attributes.get("LocalAppID").stringValue();
        String inputUrl = m.body();
        //download image
        String imagePath = downloadImage(inputUrl);
        if (imagePath.equals("")) {
            System.out.println("Error: Image not downloaded.... continuing to next ocr task, URL: " +  inputUrl);
            sendException(workersToManagerQueueUrl, localAppID, inputUrl, "Image download error");
            deleteMessageFromQueue(m, managerToWorkersQueueUrl);
            return;
        }
        //apply ocr on the image
        String ocrResult = applyOcr(imagePath, tesseract);
        if(ocrResult == null){
            System.out.println("Error during OCR operation.... continuing to next ocr task, URL: "+ inputUrl);
            sendException(workersToManagerQueueUrl, localAppID, inputUrl, "OCR operation error");
            deleteMessageFromQueue(m, managerToWorkersQueueUrl);
            return;
        }
        //send ocr result to manager
        HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
        attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
        attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
        attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(localAppID).build());
        attributesMap.put("Url", MessageAttributeValue.builder().dataType("String").stringValue(inputUrl).build());
        if(!sqs.sendMessage(workersToManagerQueueUrl, ocrResult, attributesMap)) {
            System.out.println("Error at sending OCR task result to manager, URL: " + inputUrl);
            System.exit(1); // Fatal Error
        }

        //delete ocr task message from queue - only if OCR was successful!
        deleteMessageFromQueue(m, managerToWorkersQueueUrl);

        if(!new File(imagePath).delete()){
            System.out.println("Image can't be deleted");
        }
    }

    private static void deleteMessageFromQueue(Message m, String managerToWorkersQueueUrl) {
        List<Message> msgToDelete = new ArrayList<>();
        msgToDelete.add(m);
        if(!sqs.deleteMessages(msgToDelete, managerToWorkersQueueUrl)){
            System.out.println("Error at deleting task message from managerToWorkersQueue");
            System.exit(1); // Fatal Error
        }
    }

    //send worker exception notification to manager
    private static void sendException(String workersToManagerQueueUrl, String localAppID, String inputUrl, String errorMessage) {
        HashMap<String, MessageAttributeValue> attributesMap = new HashMap<>();
        attributesMap.put("From", MessageAttributeValue.builder().dataType("String").stringValue("Worker").build());
        attributesMap.put("To", MessageAttributeValue.builder().dataType("String").stringValue("Manager").build());
        attributesMap.put("LocalAppID", MessageAttributeValue.builder().dataType("String").stringValue(localAppID).build());
        attributesMap.put("Url", MessageAttributeValue.builder().dataType("String").stringValue(inputUrl).build());
        attributesMap.put("ExceptionSummary", MessageAttributeValue.builder().dataType("String").stringValue(errorMessage).build());
        if (!sqs.sendMessage(workersToManagerQueueUrl, "WORKER EXCEPTION", attributesMap)) {
            System.out.println("Error at sending worker exception to manager");
        }
    }

    // TODO sudo apt-get tesseract-ocr
    // TODO call tess.setDataPath() to point to your tesseract installation (/usr/share/tesseract-ocr/ for my Ubuntu 14.04)

    private static String applyOcr(String imagePath, Tesseract tesseract){
        try {
            //TODO: check if setDataPath should be the path of the downloaded image
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
        String downloadFilePath = GeneralUtils.getUniqueID() + "__Image.png";
        BufferedImage image = null;
        try {
            URL url = new URL(urlInput);
            image = ImageIO.read(url);
            if(image == null){
                System.out.println("Error at downloadImage: image can't be downloaded");
                return "";
            }
            ImageIO.write(image, "png",new File(downloadFilePath) );
        } catch (IOException e) {
            System.out.println("Error at downloadImage: broken link");
            e.printStackTrace();
            return "";
        }
        return downloadFilePath;

//        try{
//            URL url = new URL(urlInput);
//            InputStream inputStream = url.openStream();
//            OutputStream fileOutputStream = new FileOutputStream(downloadFilePath);
//            int ch;
//            while ((ch = inputStream.read()) != -1) { //read till end of file
//                fileOutputStream.write(ch);
//            }
//            inputStream.close();
//            fileOutputStream.close();
//            return downloadFilePath;
//        }
//        catch (IOException e){
//            System.out.println("Error at creating url object");
//            return "";
//        }
    }


}
