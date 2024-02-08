package aws;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import static aws.AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME;

public class S3Handler {
    private final S3Client s3 = S3Client.builder().region(AWSConfig.REGION1).build();

    public void uploadFileToS3(String bucketName, File inputFile) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(AWSConfig.BUCKET_NAME)
                .key(inputFile.getName())
                .build();
        s3.putObject(objectRequest, RequestBody.fromFile(inputFile));
    }

    public void uploadContentToS3(String bucketName, String key, String content) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3.putObject(objectRequest, RequestBody.fromString(content));
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
}
