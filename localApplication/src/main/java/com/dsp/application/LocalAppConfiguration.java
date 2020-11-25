package com.dsp.application;

import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class LocalAppConfiguration {

    private String s3BucketName;
    private String localToManagerQueueName;
    private String managerToLocalQueueName;
    private String arn;
    private String awsKeyPair;

    private InstanceType instanceType;

    private String ami;

    public LocalAppConfiguration() {
        s3BucketName = "locals_manager_shared_bucket_1586521648532"; //TODO change names
        localToManagerQueueName = "localToManager_1586521648532";
        managerToLocalQueueName = "managerToLocal_1586521648532";
        arn = "arn:aws:iam::322970830450:instance-profile/ec2_role_full_access";
        awsKeyPair = "ec2key_dsp1";
        ami = "ami-076515f20540e6e0b";
        instanceType = InstanceType.T2_MICRO;
        readConfigFile();
    }

    //TODO: read config from file
    private static void readConfigFile(){

    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getLocalToManagerQueueName() {
        return localToManagerQueueName;
    }

    public String getManagerToLocalQueueName() {
        return managerToLocalQueueName;
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

