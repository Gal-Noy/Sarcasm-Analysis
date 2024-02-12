import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SQSHandler {

    private final SqsClient sqs = SqsClient.builder().region(AWSConfig.REGION1).build();
    private final Logger logger = LogManager.getLogger(SQSHandler.class);

    public String createQueueIfNotExist(String queueName) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse createResult = sqs.createQueue(request);
            logger.info("Queue " + queueName + " created");
            return createResult.queueUrl();
        }
        catch (QueueNameExistsException e) {
            return getQueueUrl(queueName);
        }
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(request).queueUrl();
    }

    public String getLocalAppNameFromQueueUrl(String queueUrl) {
        return queueUrl.split("/")[4].split("-")[1];
    }

    public List<String> getAllQueuesByPrefix(String queueNamePrefix) {
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest
                    .builder().queueNamePrefix(queueNamePrefix).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
            return listQueuesResponse.queueUrls();
        }
        catch (SqsException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        return null;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest request = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(request);

        logger.info("Queue " + queueUrl + " deleted");
    }

    public void deleteAllQueues() {
        ListQueuesRequest listQueuesRequest = ListQueuesRequest
                .builder().queueNamePrefix("").build();
        ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
        for (String queueUrl : listQueuesResponse.queueUrls()) {
            deleteQueue(queueUrl);
        }
    }

    public void sendMessage(String queueUrl, String message) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build());

        logger.info("Sent message to " + queueUrl);
    }

    public List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(AWSConfig.LONG_POLLING_TIME) // long polling
                .visibilityTimeout(AWSConfig.VISIBILITY_TIMEOUT) // prevents the same message from being delivered to multiple workers
                .build();
        return sqs.receiveMessage(request).messages();
    }

    public Message receiveSingleMessage(String queueUrl) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(AWSConfig.LONG_POLLING_TIME) // long polling
                .visibilityTimeout(AWSConfig.VISIBILITY_TIMEOUT) // prevents the same message from being delivered to multiple workers
                .build();
        List<Message> messages = sqs.receiveMessage(request).messages();
        return messages.isEmpty() ? null : messages.get(0);
    }

    public void changeMessageVisibility(String queueUrl, Message message, int visibilityTimeout) {
        ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .visibilityTimeout(visibilityTimeout)
                .build();
        sqs.changeMessageVisibility(request);
    }

    public void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(request);

        logger.info("Deleted message from " + queueUrl);
    }
}
