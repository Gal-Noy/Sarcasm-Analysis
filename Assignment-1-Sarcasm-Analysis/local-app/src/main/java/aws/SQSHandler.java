package aws;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SQSHandler {

    private final SqsClient sqs = SqsClient.builder().region(AWSConfig.REGION).build();

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

    public void sendMessages(String queueName, List<String> messages) {
        for (String message : messages) {
            sendMessage(queueName, message);
        }
    }

    public List<Message> receiveMessages(String queueName) {
        String queueUrl = getQueueUrl(queueName);
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(20) // long polling
                .build();
        return sqs.receiveMessage(request).messages();
    }

    public void deleteMessages(String queueName, List<Message> messages) {
        for (Message message : messages) {
            deleteMessage(queueName, message);
        }
    }

    private void deleteMessage(String queueName, Message message) {
        String queueUrl = getQueueUrl(queueName);
        // create message instance
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(request);
    }

    public void deleteAllMessages(String queueName) {
        String queueUrl = getQueueUrl(queueName);
        List<Message> messages = receiveMessages(queueName);
        for (Message message : messages) {
            deleteMessage(queueName, message);
        }
    }

    public void deleteQueue(String queueName) {
        DeleteQueueRequest request = DeleteQueueRequest.builder()
                .queueUrl(queueName)
                .build();
        sqs.deleteQueue(request);
    }

}
