package com.dsp.aws;

import com.dsp.utils.GeneralUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class S3client {

    private static final Region REGION = Region.US_EAST_1;
    private final S3Client s3;
    private final GeneralUtils generalUtils;

    public S3client() {
        generalUtils = new GeneralUtils();
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
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    public boolean deleteBucket(String bucketName){
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest
                .builder()
                .bucket(bucketName)
                .build();
        try {
            s3.deleteBucket(deleteBucketRequest);
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    // add a key/value pair to an S3 bucket from given file located in inFilePath.
    public boolean putObject(String bucketName, String bucketKey, String inFilePath) {
        PutObjectRequest putRequest = PutObjectRequest
                                    .builder()
                                    .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
                                    .bucket(bucketName).key(bucketKey)
                                    .build();
        try {
            generalUtils.logPrint("putting file in s3 bucket");
            s3.putObject(putRequest, RequestBody.fromFile(new File(inFilePath)));
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        generalUtils.logPrint("done putting file in s3 bucket");
        return true;
    }

    // add a key/value pair to an S3 bucket from given file located in inFilePath.
    public boolean putObjectFromMemory(String bucketName, String bucketKey, String value) {
        PutObjectRequest putRequest = PutObjectRequest
                .builder()
                .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
                .bucket(bucketName).key(bucketKey)
                .build();
        try {
            generalUtils.logPrint("putting file in s3 bucket");
            s3.putObject(putRequest, RequestBody.fromString(value));
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        generalUtils.logPrint("done putting file in s3 bucket");
        return true;
    }

    public boolean deleteObject(String bucketName, String bucketKey){
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest
                                                .builder()
                                                .bucket(bucketName)
                                                .key(bucketKey)
                                                .build();
        try{
            generalUtils.logPrint("deleting file from s3 bucket: " + bucketKey);
            s3.deleteObject(deleteObjectRequest);
        } catch(Exception e){
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    // read the value of bucketKey in the S3 bucket and save it to the file in outFilePath
    public boolean getObject(String bucket, String bucketKey, String outFilePath) {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucket).key(bucketKey).build();
        try {
            s3.getObject(getRequest, ResponseTransformer.toFile(Paths.get(outFilePath)));
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return false;
        }
        return true;
    }

    // read the value of bucketKey in the S3 bucket and save it to the file in outFilePath
    public String getObjectToMemory(String bucket, String bucketKey) {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucket).key(bucketKey).build();
        ResponseBytes<GetObjectResponse> result;
        try {
            result = s3.getObject(getRequest, ResponseTransformer.toBytes());
        } catch (Exception e) {
            GeneralUtils.printStackTrace(e, generalUtils);
            return null;
        }
        return new String(result.asByteArray());
    }

    public List<String> getAllObjectsKeys(String bucket, String prefix){
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();
        ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);

        List<String> keys = new ArrayList<>();
        for (ListObjectsV2Response page : response) {
            page.contents().forEach((S3Object object) -> {
                keys.add(object.key());
            });
        }
        return keys;
    }

    public List<String> getAllBucketNames(){
        ListBucketsResponse listBucketsResponse = s3.listBuckets();
        return listBucketsResponse
                .buckets()
                .stream()
                .map(Bucket::name)
                .collect(Collectors.toList());
    }
}

