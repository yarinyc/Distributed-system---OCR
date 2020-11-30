package com.dsp.aws;

import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.List;

public class EC2Client {

    private static final Region REGION = Region.US_EAST_1;
    private final Ec2Client ec2client;
    private final GeneralUtils generalUtils;

    public EC2Client() {
        generalUtils = new GeneralUtils();
        this.ec2client = Ec2Client
                        .builder()
                        .region(REGION)
                        .build();
    }

    // return a list of all ec2 instances created.
    // keyName: private key pair of AWS EC2. arn: (amazon resource name)-predefined IAM Role.
    public List<Instance> createEC2Instances(String amiId, String keyName, int minCount, int maxCount, String userData, String arn, InstanceType instanceType) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(instanceType)
                    .keyName(keyName)
                    .maxCount(minCount)
                    .minCount(maxCount)
                    .userData(userData)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(arn).build())
                    .build();
            try {
                RunInstancesResponse response = ec2client.runInstances(runRequest);
                return response.instances();
            } catch (Exception e) {
                GeneralUtils.printStackTrace(e, generalUtils);
                return null;
            }
    }

    // returns a list of all EC2 instances that match the filter.
    public List<Instance> getAllInstances(Filter filter) {
        String nextToken = null;
        List<Instance> instances = new ArrayList<>();
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest
                                                    .builder()
                                                    .filters(filter)
                                                    .nextToken(nextToken)
                                                    .build();
                DescribeInstancesResponse response = ec2client.describeInstances(request);
                // find all instances in current response
                for (Reservation reservation : response.reservations()) {
                    instances.addAll(reservation.instances());
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
        }
        return instances;
    }

    @SuppressWarnings("unused")
    public int getNumberOfInstances(Filter filter) {
        String nextToken = null;
        int count = 0;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest
                        .builder()
                        .filters(filter)
                        .nextToken(nextToken)
                        .build();
                DescribeInstancesResponse response = ec2client.describeInstances(request);
                // find all instances in current response
                for (Reservation reservation : response.reservations()) {
                    count += reservation.instances().size();
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
        }
        return count;
    }

    // tag an EC2 instance.
    public boolean createTag(String tagName, String tagValue, String instanceId) {
        Tag tag = Tag.builder()
                .key(tagName)
                .value(tagValue)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        try {
            ec2client.createTags(tagRequest);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    // terminate all instances specified by their ID's
    public boolean terminateInstances(List<String> instancesID) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest
                .builder().instanceIds(instancesID)
                .build();
        try {
            ec2client.terminateInstances(terminateRequest);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }
}
