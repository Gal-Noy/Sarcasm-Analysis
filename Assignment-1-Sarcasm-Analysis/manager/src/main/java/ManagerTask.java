import java.util.List;
import java.util.Map;

public class ManagerTask implements Runnable {
    private final AWS aws = AWS.getInstance();
    private final String localAppId;
    private final String inputFileName;
    private final String inputIndex;
    private final Map<String, Review> requestReviews;
    private final String bucketName;
    private int tasksSent = 0;
    private int tasksCompleted = 0;
    private final StringBuilder summaryMessage;

    public ManagerTask(String localAppId, String inputFileName, String inputIndex, Map<String, Review> requestReviews, String bucketName) {
        this.localAppId = localAppId;
        this.inputFileName = inputFileName;
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

        receiveTasksFromWorkers();

        System.out.println("[DEBUG] All tasks completed, sending summary to local app");

        handleSummary();

        System.out.printf("[DEBUG] ManagerTask finished for localAppId %s\n", localAppId);
    }

    private void sendTasksToWorkers() {
        String managerToWorkerQueueUrl = aws.sqs.getQueueUrl(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + "::" + localAppId);

        for (Map.Entry<String, Review> entry : requestReviews.entrySet()) {
            String reviewId = entry.getKey();
            String reviewText = entry.getValue().getText();

            // <local_app_id>::<task_type>::<input_file>::<input_index>::<review_id>
            String sentimentTask = String.join("::",
                    localAppId, AWSConfig.SENTIMENT_ANALYSIS_TASK, inputIndex, reviewId);
            String entityTask = String.join("::",
                    localAppId, AWSConfig.ENTITY_RECOGNITION_TASK, inputFileName, inputIndex, reviewId);

            aws.sqs.sendMessage(managerToWorkerQueueUrl, sentimentTask);
            aws.sqs.sendMessage(managerToWorkerQueueUrl, entityTask);

            tasksSent += 2;
        }
    }

    private void receiveTasksFromWorkers() {
        String workerToManagerQueueUrl = aws.sqs.getQueueUrl(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + "::" + localAppId);

        List<String> responses = aws.sqs.receiveMessages(workerToManagerQueueUrl);
        while (tasksCompleted < tasksSent) {
            for (String response : responses) {
                // <local_app_id>::<task_type>::<task_result>::<input_index>::<review_id>
                String[] responseContent = response.split("::");
                String localAppId = responseContent[0], taskType = responseContent[1], taskResult = responseContent[2],
                        inputIndex = responseContent[3], reviewId = responseContent[4];

                if (localAppId.equals(this.localAppId) && inputIndex.equals(this.inputIndex)) {
                    int reviewRating = requestReviews.get(reviewId).getRating();
                    String reviewLink = requestReviews.get(reviewId).getLink();
                    summaryMessage.append(String.join("::",
                            reviewId, reviewRating + "", reviewLink, taskType, taskResult))
                            .append(";;");
                }

                tasksCompleted++;
                aws.sqs.deleteMessage(workerToManagerQueueUrl, response);
            }
        }
    }

    private void handleSummary() {
        System.out.printf("[DEBUG] Handling summary for localAppId %s\n", localAppId);

        String summaryFileName = String.join("::", localAppId, "summary", inputIndex); // S3 bucket key
        aws.s3.uploadContentToS3(bucketName, summaryFileName, summaryMessage.toString());

        String managerToLocalQueueUrl = aws.sqs.getQueueUrl(AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + "::" + localAppId);
        // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
        String responseContent = String.join("::", localAppId, AWSConfig.RESPONSE_STATUS_DONE, summaryFileName, inputIndex);
        aws.sqs.sendMessage(managerToLocalQueueUrl, responseContent);

        System.out.printf("[DEBUG] Sent summary to local app for inputIndex %s\n", inputIndex);
    }
}