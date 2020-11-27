package com.dsp.application;

import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class LocalAppConfiguration {

    private String s3BucketName;
    private String localToManagerQueueName;
    private String arn;
    private String awsKeyPair;

    private InstanceType instanceType;

    private String ami;

    public LocalAppConfiguration() {
        instanceType = InstanceType.T2_MICRO;
        readConfigFile();
    }

    //TODO: read config from file
    private void readConfigFile(){
        List<String> conf;
        try {
            conf = Files.readAllLines(Paths.get("resources", "config.txt"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        if(conf.size() < 5 ){
            System.out.println("Error in readConfigFile: bad config file");
            System.exit(1);
        }
        s3BucketName = conf.get(0);
        localToManagerQueueName = conf.get(1);
        arn = conf.get(2);
        awsKeyPair = conf.get(3);
        ami = conf.get(4);
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getLocalToManagerQueueName() {
        return localToManagerQueueName;
    }

    public String getArn() {
        return arn;
    }

    public String getAwsKeyPair() {
        return awsKeyPair;
    }

    public String getAmi() {
        return ami;
    }

    public InstanceType getInstanceType() {
        return instanceType;
    }
}

