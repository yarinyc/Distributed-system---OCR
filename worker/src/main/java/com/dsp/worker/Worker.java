package com.dsp.worker;

import com.dsp.utils.GeneralUtils;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.List;
import javax.imageio.ImageIO;

import com.dsp.aws.SQSClient;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;


public class Worker {
    private static SQSClient sqs;
    private static Tesseract tesseract;
    private static GeneralUtils generalUtils;

    public static void main(String[] args) {

        //get queue urls from args
        String managerToWorkersQueueUrl = args[0];
        String workersToManagerQueueUrl = args[1];

        generalUtils = new GeneralUtils();

        //init AWS clients
        sqs = new SQSClient();

        //create OCR engine
        tesseract = new Tesseract();
        // TODO call tess.setDataPath() to point to your tesseract installation (/usr/share/tesseract-ocr/ for my Ubuntu 14.04)
        // TODO check this is the correct path for tess after apt-get
        tesseract.setDatapath("/usr/share/tesseract-ocr/4.00/tessdata");

        while(!Thread.interrupted()){
            List<Message> messages = sqs.getMessages(managerToWorkersQueueUrl, 1);
            for(Message m : messages){
                handleOcrTask(m,workersToManagerQueueUrl, managerToWorkersQueueUrl);
            }
        }
        generalUtils.logPrint("Worker finished");
    }

    //handle OCR Task
    private static void handleOcrTask(Message m, String workersToManagerQueueUrl, String managerToWorkersQueueUrl) {
        Map<String, MessageAttributeValue> attributes = m.messageAttributes();
        String localAppID = attributes.get("LocalAppID").stringValue();
        String inputUrl = m.body();
        //download image
        String imagePath = downloadImage(inputUrl);
        if (imagePath.equals("")) {
            generalUtils.logPrint("Error: Image not downloaded.... continuing to next ocr task, URL: " +  inputUrl);
            sendException(workersToManagerQueueUrl, localAppID, inputUrl, "Image download error");
            deleteMessageFromQueue(m, managerToWorkersQueueUrl);
            return;
        }
        //apply ocr on the image
        String ocrResult = applyOcr(imagePath, tesseract);
        if(ocrResult == null){
            generalUtils.logPrint("Error during OCR operation.... continuing to next ocr task, URL: "+ inputUrl);
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
            generalUtils.logPrint("Error at sending OCR task result to manager, URL: " + inputUrl);
            System.exit(1); // Fatal Error
        }

        //delete ocr task message from queue - only if OCR was successful!
        deleteMessageFromQueue(m, managerToWorkersQueueUrl);

        if(!new File(imagePath).delete()){
            generalUtils.logPrint("Image can't be deleted");
        }
    }

    private static void deleteMessageFromQueue(Message m, String managerToWorkersQueueUrl) {
        List<Message> msgToDelete = new ArrayList<>();
        msgToDelete.add(m);
        if(!sqs.deleteMessages(msgToDelete, managerToWorkersQueueUrl)){
            generalUtils.logPrint("Error at deleting task message from managerToWorkersQueue");
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
            generalUtils.logPrint("Error at sending worker exception to manager");
        }
    }

    private static String applyOcr(String imagePath, Tesseract tesseract){
        try {
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
        try {
            URL url = new URL(urlInput);
            BufferedImage image = ImageIO.read(url);
            if(image == null){
                generalUtils.logPrint("Error at downloadImage: image can't be downloaded");
                return "";
            }
            ImageIO.write(image, "png",new File(downloadFilePath) );
        } catch (IOException e) {
            generalUtils.logPrint("Error at downloadImage: broken link");
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
//            generalUtils.logPrint("Error at creating url object");
//            return "";
//        }
    }


}
