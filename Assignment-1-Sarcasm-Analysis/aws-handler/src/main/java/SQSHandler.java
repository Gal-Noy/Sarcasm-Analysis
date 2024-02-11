import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SQSHandler {

    private final SqsClient sqs = SqsClient.builder().region(AWSConfig.REGION1).build();
    final Logger logger;

    public SQSHandler(Logger logger) {
        this.logger = logger;
    }

    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(request);

        logger.info("Queue " + queueName + " created");

        return getQueueUrl(queueName);
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
        logger.info("Polling messages from " + queueUrl);

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20) // long polling
                .visibilityTimeout(60) // prevents the same message from being delivered to multiple workers
                .build();
        return sqs.receiveMessage(request).messages();
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
