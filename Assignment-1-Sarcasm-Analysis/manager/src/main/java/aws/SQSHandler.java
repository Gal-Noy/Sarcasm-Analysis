package aws;

import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

import static aws.AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME;

public class SQSHandler {

    private final SqsClient sqs = SqsClient.builder().region(AWSConfig.REGION1).build();

    public void createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(request);
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(request).queueUrl();
    }

    public void sendMessage(String queueName, String message) {
        String queueUrl = getQueueUrl(queueName);
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build());
    }

    public List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(20) // long polling
                .build();
        return sqs.receiveMessage(request).messages();
    }

    public void deleteQueue(String queueName) {
        DeleteQueueRequest request = DeleteQueueRequest.builder()
                .queueUrl(queueName)
                .build();
        sqs.deleteQueue(request);
    }

    public List<String> getAllLocalToManagerQueues() {
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest
                    .builder().queueNamePrefix(LOCAL_TO_MANAGER_QUEUE_NAME).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
            return listQueuesResponse.queueUrls();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void deleteMessage(String queueUrl, String message) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message)
                .build();
        sqs.deleteMessage(request);
    }

}
