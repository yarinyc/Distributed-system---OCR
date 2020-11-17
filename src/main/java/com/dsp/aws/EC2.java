package com.dsp.aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.List;

public class EC2 {
    private Ec2Client ec2;

    public EC2() {
        ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
    }

    public List<Instance> createInstances(String amiId, String keyName, int minCount, int maxCount, String userData, String arn) {
        {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(InstanceType.T2_MICRO)
                    .keyName(keyName)
                    .maxCount(minCount)
                    .minCount(maxCount)
                    .userData(userData)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(arn).build())
                    .build();
            try {
                RunInstancesResponse response = ec2.runInstances(runRequest);
                return response.instances();
            } catch (Exception e) {
                return null;
            }

        }
    }

    public List<Instance> getAllInstances(Filter filter) {
        String nextToken = null;
        List<Instance> instances = new ArrayList<>();
        try {
            do {
                DescribeInstancesRequest request =
                        DescribeInstancesRequest.builder().filters(filter).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        instances.add(instance);
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            System.out.println(e.getMessage());
        }
        return instances;
    }

    public boolean createTag(String tagName, String instanceName, String instanceId) {
        Tag tag = Tag.builder()
                .key(tagName)
                .value(instanceName)      //define instance name
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;

    }
}
