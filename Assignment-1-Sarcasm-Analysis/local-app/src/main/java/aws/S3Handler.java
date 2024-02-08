package aws;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

public class S3Handler {
    private final S3Client s3 = S3Client.builder().region(AWSConfig.REGION1).build();

    public void createS3Bucket(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println("[DEBUG] S3 bucket " + bucketName + " created");
        } catch (S3Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }


    public void uploadFileToS3(String bucketName, File inputFile) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(AWSConfig.BUCKET_NAME)
                .key(inputFile.getName())
                .build();
        s3.putObject(objectRequest, RequestBody.fromFile(inputFile));
    }

    public InputStream downloadFileFromS3(String bucketName, String key) {
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        return s3.getObject(objectRequest, ResponseTransformer.toBytes()).asInputStream();
    }

    public void deleteFileFromS3(String bucketName, String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3.deleteObject(deleteObjectRequest);
    }

    public void emptyS3Bucket(String bucketName) {
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(bucketName)
                .build();
        ListObjectsResponse listResponse = s3.listObjects(listObjects);
        for (S3Object s3Object : listResponse.contents()) {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Object.key())
                    .build());
        }
    }

    public void deleteS3Bucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3.deleteBucket(deleteBucketRequest);
    }
}
