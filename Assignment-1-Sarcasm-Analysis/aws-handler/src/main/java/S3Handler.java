import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

public class S3Handler {
    private final S3Client s3 = S3Client.builder().region(AWSConfig.REGION2).build();
    final Logger logger;

    public S3Handler(Logger logger) {
        this.logger = logger;
    }

    public void createS3BucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            logger.info("S3 bucket " + bucketName + " created");
        } catch (S3Exception e) {
            if (!e.getMessage().contains("BucketAlreadyOwnedByYou")) {
                logger.error(e.getMessage());
            } else {
                logger.info("S3 bucket " + bucketName + " already exists");
            }
        }
    }


    public void uploadFileToS3(String bucketName, File inputFile) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(inputFile.getName())
                .build();
        s3.putObject(objectRequest, RequestBody.fromFile(inputFile));

        logger.info("File " + inputFile.getName() + " uploaded to S3 bucket " + bucketName);
    }

    public void uploadContentToS3(String bucketName, String key, String content) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3.putObject(objectRequest, RequestBody.fromString(content));

        logger.info("Content uploaded to S3 bucket " + bucketName + " with key " + key);
    }

    public InputStream downloadFileFromS3(String bucketName, String key) {
        logger.info("Downloading file " + key + " from S3 bucket " + bucketName);
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

        logger.info("File " + key + " deleted from S3 bucket " + bucketName);
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

        logger.info("S3 bucket " + bucketName + " emptied");
    }

    public void deleteS3Bucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3.deleteBucket(deleteBucketRequest);

        logger.info("S3 bucket " + bucketName + " deleted");
    }

    public void emptyAndDeleteAllBuckets() {
        ListBucketsResponse listBucketsResponse = s3.listBuckets();
        for (Bucket bucket : listBucketsResponse.buckets()) {
            if (!bucket.name().equals(AWSConfig.JARS_BUCKET_NAME)) {
                emptyS3Bucket(bucket.name());
                deleteS3Bucket(bucket.name());
            }
        }
    }
}
