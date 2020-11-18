package com.dsp.application;

import com.dsp.aws.EC2Client;
import com.dsp.aws.S3client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;

//ec2 role: ec2roledsp1
//aws arn: arn:aws:iam::322970830450:instance-profile/ec2_role_full_access

public class LocalApplication
{
    public static void main( String[] args ){
//        EC2Client ec2Client = new EC2Client();
//        String arn = "arn:aws:iam::322970830450:instance-profile/ec2_role_full_access";
//        String amiId = "ami-076515f20540e6e0b";
//        InstanceType instanceType = InstanceType.T2_MICRO;
//        List<Instance> instances = ec2Client.createEC2Instances(amiId, "ec2key_dsp1", 1, 1, createManagerScript(), arn, instanceType);

        S3client client = new S3client();
        client.createBucket("aaabbbcccguru");
        client.putObject("aaabbbcccguru", "omerkey", "C:\\Users\\ycohen\\Desktop\\omerfile.txt");
        client.getObject("aaabbbcccguru", "omerkey", "C:\\Users\\ycohen\\Desktop\\out.txt");
        System.out.println("done");



    }
    private static String createManagerScript() {
        String userData = "";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo mkdir yarintry";
        return toBase64(userData);
    }

    private static String toBase64(String data) {
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(data.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64UserData;
    }
}
