package manager;

import aws.AWS;
import static aws.AWSConfig.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sqs.model.Message;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManagerTask implements Runnable {
    private final AWS aws = AWS.getInstance();
    private final ManagerEnv env = ManagerEnv.getInstance();
    private final String localAppId;
    private final String inputIndex;
    private final Map<String, Review> requestReviews;
    private int tasksSent = 0;
    private int tasksCompleted = 0;
    private final Map<String, String> reviewsSentiment;
    private final Map<String, String> reviewsEntities;
    private final StringBuilder summaryMessage;
    private final Logger logger = LogManager.getLogger(ManagerTask.class);
    private final int workersToRelease;

    public ManagerTask(String localAppId, String inputIndex, Map<String, Review> requestReviews, int workersToRelease) {
        this.localAppId = localAppId;
        this.inputIndex = inputIndex;
        this.requestReviews = requestReviews;
        this.reviewsSentiment = new HashMap<>();
        this.reviewsEntities = new HashMap<>();
        this.summaryMessage = new StringBuilder();
        this.workersToRelease = workersToRelease;
    }


    @Override
    public void run() {
        logger.info("ManagerTask started for local app " + localAppId + " for inputIndex " + inputIndex);

        sendTasksToWorkers();

        logger.info("Sent tasks to workers for local app " + localAppId + " for inputIndex " + inputIndex);

        receiveResponsesFromWorkers();

        logger.info("Received all responses from workers for local app " + localAppId + " for inputIndex " + inputIndex);

        handleSummary();

        logger.info("Finished summary for local app " + localAppId + " for inputIndex " + inputIndex);

        env.releaseWorkers(workersToRelease);

        logger.info("ManagerTask finished for local app " + localAppId + " for inputIndex " + inputIndex);
    }

    private void sendTasksToWorkers() {
        logger.info("Sending tasks to workers for local app " + localAppId + " for inputIndex " + inputIndex);

        String managerToWorkerQueueUrl = aws.sqs.getQueueUrl(MANAGER_TO_WORKER_QUEUE_NAME);

        for (Map.Entry<String, Review> entry : requestReviews.entrySet()) {
            String reviewId = entry.getKey();
            String reviewText = entry.getValue().getText();

            // <local_app_id>::<input_index>::<review_id>::<review_text>::<task_type>
            String sentimentTask = String.join(MESSAGE_DELIMITER, localAppId, inputIndex, reviewId, reviewText, SENTIMENT_ANALYSIS_TASK);
            String entitiesTask = String.join(MESSAGE_DELIMITER, localAppId, inputIndex, reviewId, reviewText, ENTITY_RECOGNITION_TASK);

            aws.sqs.sendMessage(managerToWorkerQueueUrl, sentimentTask);
            aws.sqs.sendMessage(managerToWorkerQueueUrl, entitiesTask);

            tasksSent += 2;

            logger.info("Sent tasks for reviewId " + reviewId + " for local app " + localAppId + " for inputIndex " + inputIndex);
            logger.info("Sent total tasks " + tasksSent + " to workers for local app " + localAppId + " for inputIndex " + inputIndex);
        }
    }

    private void receiveResponsesFromWorkers() {
        logger.info("Receiving responses from workers for local app " + localAppId + " for inputIndex " + inputIndex);

        String workerToManagerQueueUrl = aws.sqs.getQueueUrl(WORKER_TO_MANAGER_QUEUE_NAME);

        while (tasksCompleted < tasksSent) {
            logger.info("Polling responses from " + WORKER_TO_MANAGER_QUEUE_NAME);
            List<Message> responses = aws.sqs.receiveMessages(workerToManagerQueueUrl); // long polling

            for (Message response : responses) {
                String responseBody = response.body();
                // <local_app_id>::<input_index>::<review_id>::<task_type>::<task_result>
                String[] responseContent = responseBody.split(MESSAGE_DELIMITER, -1);
                String localAppId = responseContent[0], inputIndex = responseContent[1],
                        reviewId = responseContent[2], taskType = responseContent[3], taskResult = responseContent[4];

                // Check if response is for this task
                if (localAppId.equals(this.localAppId) && inputIndex.equals(this.inputIndex)) {
                    logger.info("Received response for reviewId " + reviewId + " for inputIndex " + inputIndex + " for taskType " + taskType + " with taskResult " + taskResult);

                    if (taskType.equals(SENTIMENT_ANALYSIS_TASK)) {
                        reviewsSentiment.put(reviewId, taskResult);
                    }

                    if (taskType.equals(ENTITY_RECOGNITION_TASK)) {
                        reviewsEntities.put(reviewId, taskResult);
                    }

                    if (reviewsSentiment.containsKey(reviewId) && reviewsEntities.containsKey(reviewId)) {
                        String sentiment = reviewsSentiment.get(reviewId);
                        String entities = reviewsEntities.get(reviewId);
                        int reviewRating = requestReviews.get(reviewId).getRating();
                        String reviewLink = requestReviews.get(reviewId).getLink();

                        if (summaryMessage.length() > 0){
                            summaryMessage.append(SUMMARY_DELIMITER);
                        }

                        // ...##<review_id>::<review_rating>::<review_link>::<sentiment>::<entities>##...
                        summaryMessage.append(String.join(MESSAGE_DELIMITER,
                                reviewId, reviewRating + "", reviewLink, sentiment, entities));

                        logger.info("Updated summary message for reviewId " + reviewId + " for inputIndex " + inputIndex + " with sentiment " + sentiment + " and entities " + entities);
                    }

                    tasksCompleted++;
                    aws.sqs.deleteMessage(workerToManagerQueueUrl, response);
                    logger.info("Completed tasks " + tasksCompleted + " out of " + tasksSent + " for local app " + localAppId + " for inputIndex " + inputIndex);
                }
                else {
                    // Put back in queue
                    logger.info("Putting back not relevant response in workerToManager queue");
                    aws.sqs.changeMessageVisibility(workerToManagerQueueUrl, response, RETURN_TASK_TIME);
                }
            }
        }
    }

    private void handleSummary() {
        // <local_app_id>-summary-<input_index>
        String summaryFileName = String.join(DEFAULT_DELIMITER, localAppId, SUMMARY_FILE_INDICATOR, inputIndex);

        aws.s3.uploadContentToS3(BUCKET_NAME,
                localAppId + BUCKET_KEY_DELIMITER + summaryFileName,
                summaryMessage.toString());

        // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
        String responseContent = String.join(MESSAGE_DELIMITER, localAppId, RESPONSE_STATUS_DONE, summaryFileName, inputIndex);
        aws.sqs.sendMessage(aws.sqs.getQueueUrl(MANAGER_TO_LOCAL_QUEUE_NAME), responseContent);

        logger.info("Uploaded summary file " + summaryFileName + " to S3 and sent response to local app " + localAppId + " for inputIndex " + inputIndex);
    }
}