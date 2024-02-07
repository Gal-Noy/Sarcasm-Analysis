package aws;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

public class S3Handler {
    private final S3Client s3 = S3Client.builder().region(AWSConfig.REGION).build();

    public void createS3Bucket() {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(AWSConfig.BUCKET_NAME)
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(AWSConfig.BUCKET_NAME)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public String getBucketUrl() {
        return "https://" + AWSConfig.BUCKET_NAME + ".s3." + AWSConfig.REGION.id() + ".amazonaws.com/";
    }

    public void uploadFileToS3(File inputFile) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(AWSConfig.BUCKET_NAME)
                .key(inputFile.getName())
                .build();
        s3.putObject(objectRequest, RequestBody.fromFile(inputFile));
    }

    public InputStream downloadFileFromS3(String key) {
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(AWSConfig.BUCKET_NAME)
                .key(key)
                .build();
        return s3.getObject(objectRequest, ResponseTransformer.toBytes()).asInputStream();
    }

    public void deleteFileFromS3(String key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(AWSConfig.BUCKET_NAME)
                .key(key)
                .build();
        s3.deleteObject(deleteObjectRequest);
    }

    public void deleteS3Bucket() {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(AWSConfig.BUCKET_NAME)
                .build();
        s3.deleteBucket(deleteBucketRequest);
    }
}
