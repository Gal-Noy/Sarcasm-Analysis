package aws;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

public class S3Handler {
    private final S3Client s3 = S3Client.builder().region(AWSConfig.REGION).build();

    public void createS3Bucket(String localAppId) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(AWSConfig.BUCKET_NAME + "-" + localAppId)
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(AWSConfig.BUCKET_NAME + "-" + localAppId)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void uploadFileToS3(String bucketName, String inputFile) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(inputFile)
                .build();
        s3.putObject(objectRequest, RequestBody.fromFile(new File(inputFile)));
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

    public void deleteS3Bucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3.deleteBucket(deleteBucketRequest);
    }
}
