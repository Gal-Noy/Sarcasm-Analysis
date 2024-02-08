import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SQSHandler {

    private final SqsClient sqs = SqsClient.builder().region(AWSConfig.REGION).build();

    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(request);
        System.out.println("[DEBUG] Created queue " + queueName);
        return getQueueUrl(queueName);
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(request).queueUrl();
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest request = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(request);
    }

    public List<String> getAllLocalToManagerQueues() {
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest
                    .builder().queueNamePrefix(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
            return listQueuesResponse.queueUrls();

        } catch (SqsException e) {
            System.err.printf("[ERROR] %s", e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public List<String> getAllManagerToWorkerQueues() {
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest
                    .builder().queueNamePrefix(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
            return listQueuesResponse.queueUrls();
        }
        catch (SqsException e) {
            System.err.printf("[ERROR] %s", e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void sendMessage(String queueUrl, String message) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build());
    }

    public List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10) // long polling
                .build();
        return sqs.receiveMessage(request).messages();
    }

    public Message receiveSingleMessage(String queueUrl) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();

        return sqs.receiveMessage(request).messages().get(0);
    }

    public void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(request);
    }



}
