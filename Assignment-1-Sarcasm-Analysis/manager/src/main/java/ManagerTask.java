import org.slf4j.Logger;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManagerTask implements Runnable {
    private final AWS aws = AWS.getInstance();
    private final String localAppId;
    private final String inputIndex;
    private final Map<String, Review> requestReviews;
    private final String bucketName;
    private int tasksSent = 0;
    private int tasksCompleted = 0;
    private Map<String, String> reviewsSentiment = new HashMap<>();
    private Map<String, String> reviewsEntities = new HashMap<>();
    private final StringBuilder summaryMessage;
    private final Logger logger;

    public ManagerTask(String localAppId, String inputIndex, Map<String, Review> requestReviews, String bucketName, Logger logger) {
        this.localAppId = localAppId;
        this.inputIndex = inputIndex;
        this.requestReviews = requestReviews;
        this.bucketName = bucketName;
        this.reviewsSentiment = new HashMap<>();
        this.reviewsEntities = new HashMap<>();
        this.summaryMessage = new StringBuilder();
        this.logger = logger;
    }

    @Override
    public void run() {
        logger.info("[INFO] ManagerTask started for localAppId " + localAppId);

        sendTasksToWorkers();

        logger.info("[INFO] All tasks sent to workers");

        receiveResponsesFromWorkers();

        logger.info("[INFO] All responses received from workers");

        handleSummary();

        logger.info("[INFO] Summary handled, ManagerTask finished for localAppId " + localAppId);
    }

    private void sendTasksToWorkers() {
        logger.info("[INFO] Sending tasks to workers for local app " + localAppId);

        String managerToWorkerQueueUrl = aws.sqs.getQueueUrl(
                AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId
        );

        for (Map.Entry<String, Review> entry : requestReviews.entrySet()) {
            String reviewId = entry.getKey();
            String reviewText = entry.getValue().getText();

            // <local_app_id>::<input_index>::<review_id>::<review_text>::<task_type>
            String sentimentTask = String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, inputIndex, reviewId, reviewText, AWSConfig.SENTIMENT_ANALYSIS_TASK);
            String entitiesTask = String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, inputIndex, reviewId, reviewText, AWSConfig.ENTITY_RECOGNITION_TASK);

            aws.sqs.sendMessage(managerToWorkerQueueUrl, sentimentTask);
            aws.sqs.sendMessage(managerToWorkerQueueUrl, entitiesTask);

            tasksSent += 2;

            logger.info("[INFO] Sent total " + tasksSent + " tasks to workers for local app " + localAppId + " for inputIndex " + inputIndex + " for reviewId " + reviewId);
        }
    }

    private void receiveResponsesFromWorkers() {
        logger.info("[INFO] Receiving responses from workers for local app " + localAppId);

        String workerToManagerQueueUrl = aws.sqs.getQueueUrl(
                AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId
        );

        while (tasksCompleted < tasksSent) {
            List<Message> responses = aws.sqs.receiveMessages(workerToManagerQueueUrl);

            for (Message response : responses) {
                String responseBody = response.body();
                // <local_app_id>::<input_index>::<review_id>::<task_type>::<task_result>
                String[] responseContent = responseBody.split(AWSConfig.MESSAGE_DELIMITER, -1);
                String localAppId = responseContent[0], inputIndex = responseContent[1],
                        reviewId = responseContent[2], taskType = responseContent[3], taskResult = responseContent[4];

                // Check if response is for this task
                if (localAppId.equals(this.localAppId) && inputIndex.equals(this.inputIndex)) {
                    logger.info("[INFO] Received response for local app " + localAppId + " for inputIndex " + inputIndex + " for reviewId " + reviewId + " for taskType " + taskType);

                    int reviewRating = requestReviews.get(reviewId).getRating();
                    String reviewLink = requestReviews.get(reviewId).getLink();

                    if (taskType.equals(AWSConfig.SENTIMENT_ANALYSIS_TASK)) {
                        reviewsSentiment.put(reviewId, taskResult);
                    }

                    if (taskType.equals(AWSConfig.ENTITY_RECOGNITION_TASK)) {
                        reviewsEntities.put(reviewId, taskResult);
                    }

                    if (reviewsSentiment.containsKey(reviewId) && reviewsEntities.containsKey(reviewId)) {
                        logger.info("[INFO] Received all responses for reviewId " + reviewId + " for inputIndex " + inputIndex);

                        String sentiment = reviewsSentiment.get(reviewId);
                        String entities = reviewsEntities.get(reviewId);

                        if (!summaryMessage.isEmpty()) {
                            summaryMessage.append(AWSConfig.SUMMARY_DELIMITER);
                        }

                        // <review_id>::<review_rating>::<review_link>::<sentiment>::<entities>
                        summaryMessage.append(String.join(AWSConfig.MESSAGE_DELIMITER,
                                reviewId, reviewRating + "", reviewLink, sentiment, entities));

                        logger.info("[INFO] Summary message updated for reviewId " + reviewId + " for inputIndex " + inputIndex);
                    }

                }
                tasksCompleted++;
                aws.sqs.deleteMessage(workerToManagerQueueUrl, response);

                logger.info("total tasks completed: " + tasksCompleted + " total tasks sent: " + tasksSent);
            }
        }
    }

    private void handleSummary() {
        String summaryFileName = String.join(AWSConfig.DEFAULT_DELIMITER, localAppId, AWSConfig.SUMMARY_FILE_INDICATOR, inputIndex);
        aws.s3.uploadContentToS3(bucketName, summaryFileName, summaryMessage.toString());

        String managerToLocalQueueUrl = aws.sqs.getQueueUrl(
                AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId
        );
        // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
        String responseContent = String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, AWSConfig.RESPONSE_STATUS_DONE, summaryFileName, inputIndex);
        aws.sqs.sendMessage(managerToLocalQueueUrl, responseContent);

        logger.info("[INFO] Summary message uploaded to S3 and response sent to local app " + localAppId);
    }
}