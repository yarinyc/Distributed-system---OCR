package com.dsp.aws;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class S3client {

    private static final Region REGION = Region.US_EAST_1;
    private S3Client s3;

    public S3client() {
        s3 = S3Client
            .builder()
            .region(REGION)
            .build();
    }

    // create an s3 bucket.
    public boolean createBucket(String bucketName) {
        CreateBucketRequest bucketRequest = CreateBucketRequest
                .builder()
                .acl(BucketCannedACL.PUBLIC_READ_WRITE)
                .bucket(bucketName)
                .build();
        try {
            s3.createBucket(bucketRequest);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // add a key/value pair to an S3 bucket from given file located in inFilePath.
    public boolean putObject(String bucketName, String bucketKey, String inFilePath) {
        PutObjectRequest putRequest = PutObjectRequest
                                    .builder()
                                    .acl(ObjectCannedACL.PUBLIC_READ_WRITE)
                                    .bucket(bucketName).key(bucketKey)
                                    .build();
        try {
            System.out.println("putting file in s3 bucket");
            s3.putObject(putRequest, RequestBody.fromFile(new File(inFilePath)));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        System.out.println("done putting file in s3 bucket");
        return true;
    }

    // read the value of bucketKey in the S3 bucket and save it to the file in outFilePath
    public boolean getObject(String bucket, String bucketKey, String outFilePath) {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucket).key(bucketKey).build();
        try {
            s3.getObject(getRequest, ResponseTransformer.toFile(Paths.get(outFilePath)));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public List<String> getAllBucketNames(){
        ListBucketsResponse listBucketsResponse = s3.listBuckets();
        return listBucketsResponse
                .buckets()
                .stream()
                .map(x-> x.name())
                .collect(Collectors.toList());
    }
}

