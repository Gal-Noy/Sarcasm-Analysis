import software.amazon.awssdk.services.sqs.model.Message;

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
    private final StringBuilder summaryMessage;

    public ManagerTask(String localAppId, String inputIndex, Map<String, Review> requestReviews, String bucketName) {
        this.localAppId = localAppId;
        this.inputIndex = inputIndex;
        this.requestReviews = requestReviews;
        this.bucketName = bucketName;
        this.summaryMessage = new StringBuilder();
    }

    @Override
    public void run() {
        System.out.printf("[DEBUG] ManagerTask started for localAppId %s\n", localAppId);

        sendTasksToWorkers();

        System.out.println("[DEBUG] Waiting for tasks to be completed");

        receiveResponsesFromWorkers();

        System.out.println("[DEBUG] All tasks completed, sending summary to local app");

        handleSummary();

        System.out.printf("[DEBUG] ManagerTask finished for localAppId %s\n", localAppId);
    }

    private void sendTasksToWorkers() {
        System.out.println("[DEBUG] Sending tasks to workers");
        String managerToWorkerQueueUrl = aws.sqs.getQueueUrl(
//                AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + "-" + localAppId  TODO: Uncomment this line
                AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME  // TODO: Delete this line
        );

        for (Map.Entry<String, Review> entry : requestReviews.entrySet()) {
            String reviewId = entry.getKey();
            String reviewText = entry.getValue().getText();

            // <local_app_id>::<input_index>::<review_id>::<review_text>
            String task = String.join("::", localAppId, inputIndex, reviewId, reviewText);
            aws.sqs.sendMessage(managerToWorkerQueueUrl, task);

            tasksSent++;
            System.out.println("[DEBUG] " + tasksSent + " of request number " + inputIndex);
        }
        System.out.println("[DEBUG] Sent tasks to workers");
    }

    private void receiveResponsesFromWorkers() {
        System.out.println("[DEBUG] Receiving responses from workers for local app " + localAppId);
        String workerToManagerQueueUrl = aws.sqs.getQueueUrl(
//                AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + "-" + localAppId TODO: Uncomment this line
                AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME  // TODO: Delete this line
        );

        while (tasksCompleted < tasksSent) {
            List<Message> responses = aws.sqs.receiveMessages(workerToManagerQueueUrl);
            System.out.println("[DEBUG] Received " + responses.size() + " responses from workers");
            for (Message response : responses) {
                String responseBody = response.body();
                // <local_app_id>::<sentiment>::<entities>::<input_index>::<review_id>
                String[] responseContent = responseBody.split("::");
                String localAppId = responseContent[0], sentiment = responseContent[1],
                        entities = responseContent[2], inputIndex = responseContent[3], reviewId = responseContent[4];

                // Check if response is for this task
                if (localAppId.equals(this.localAppId) && inputIndex.equals(this.inputIndex)) {
                    int reviewRating = requestReviews.get(reviewId).getRating();
                    String reviewLink = requestReviews.get(reviewId).getLink();

                    // <review_id>::<review_rating>::<review_link>::<sentiment>::<entities>##<...>
                    summaryMessage.append(String.join("::",
                                    reviewId, reviewRating + "", reviewLink, sentiment, entities));
                    if (tasksCompleted == tasksSent - 1) {
                        summaryMessage.append("##");
                    }
                }

                tasksCompleted++;
                aws.sqs.deleteMessage(workerToManagerQueueUrl, response);

                System.out.println("[DEBUG] " + tasksCompleted + " of request number " + inputIndex);
            }
        }
        System.out.println("[DEBUG] Received all responses from workers");
    }

    private void handleSummary() {
        String summaryFileName = String.join(AWSConfig.SUMMARY_FILE_NAME_DELIMITER, localAppId, "summary", inputIndex);
        aws.s3.uploadContentToS3(bucketName, summaryFileName, summaryMessage.toString());

        String managerToLocalQueueUrl = aws.sqs.getQueueUrl(
//                AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + "-" + localAppId TODO: Uncomment this line
                AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME  // TODO: Delete this line
        );
        // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
        String responseContent = String.join("::", localAppId, AWSConfig.RESPONSE_STATUS_DONE, summaryFileName, inputIndex);
        aws.sqs.sendMessage(managerToLocalQueueUrl, responseContent);

        System.out.printf("[DEBUG] Sent summary to local app for inputIndex %s\n", inputIndex);
    }
}