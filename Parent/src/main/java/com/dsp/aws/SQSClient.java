package com.dsp.aws;

import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQSClient {

    private static final Region REGION = Region.US_EAST_1;

    private final SqsClient sqs;
    private final GeneralUtils generalUtils;

    public SQSClient() {
        generalUtils = new GeneralUtils();
        sqs = SqsClient
            .builder()
            .region(REGION)
            .build();
    }

    public boolean createQueue(String queueName, String visibility) {
//        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
//        queueAttributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
//                                                .attributes(queueAttributes)
                                                .queueName(queueName)
                                                .build();
        try {
            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            Map<QueueAttributeName, String> attributeMap = new HashMap<>();
            attributeMap.put(QueueAttributeName.VISIBILITY_TIMEOUT, visibility);
            SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest
                    .builder()
                    .queueUrl(createQueueResponse.queueUrl()) //TODO check if this is needed: getQueueUrl(queueName)
                    .attributes(attributeMap)
                    .build();
            sqs.setQueueAttributes(setQueueAttributesRequest);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    public List<Message> getMessages(String queueUrl, Integer maxNumberOfMessages) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest
                                            .builder()
                                            .queueUrl(queueUrl)
                                            .messageAttributeNames("All")
                                            .maxNumberOfMessages(maxNumberOfMessages)
                                            .build();

        return sqs.receiveMessage(receiveRequest).messages();
    }


    public boolean sendMessage(String queueUrl, String messageBody, HashMap<String, MessageAttributeValue> attributes) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(attributes)
                .delaySeconds(5)
                .build();
        try {
            sqs.sendMessage(send_msg_request);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    public boolean deleteMessages(List<Message> messages, String queueUrl) {
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                                                        .queueUrl(queueUrl)
                                                        .receiptHandle(message.receiptHandle())
                                                        .build();
            try {
                sqs.deleteMessage(deleteMessageRequest);
            } catch (Exception e) {
                GeneralUtils.printStackTrace(e, generalUtils);
                return false;
            }
        }
        return true;
    }

    public boolean changeVisibility(String queueUrl, int visibilityTimeout, String s) {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder()
                                                                        .queueUrl(queueUrl)
                                                                        .visibilityTimeout(visibilityTimeout)
                                                                        .receiptHandle(s)
                                                                        .build();
        try {
            sqs.changeMessageVisibility(changeMessageVisibilityRequest);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    public int getPendingMessagesSize(String queueUrl) {
        GetQueueAttributesRequest attributesRequest = GetQueueAttributesRequest
                                                    .builder()
                                                    .queueUrl(queueUrl)
                                                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                                                    .build();
        try {
            GetQueueAttributesResponse getQueueAttributesResponse = sqs.getQueueAttributes(attributesRequest);
            return Integer.parseInt(getQueueAttributesResponse.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return -1;
        }

    }

    public boolean deleteQueue(String queueUrl) {
        //List<Message> messages = getMessages(queueUrl, 5);
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest
                                                .builder()
                                                .queueUrl(queueUrl)
                                                .build();
        try {
//            for (Message message : messages) {
//                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
//                                                            .queueUrl(queueUrl)
//                                                            .receiptHandle(message.receiptHandle())
//                                                            .build();
//                sqs.deleteMessage(deleteMessageRequest);
//            }
            sqs.deleteQueue(deleteQueueRequest);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }


}
