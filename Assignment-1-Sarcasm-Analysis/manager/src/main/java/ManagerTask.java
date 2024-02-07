import aws.AWS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Map;

import static aws.AWSConfig.*;

public class ManagerTask implements Runnable {
    private final AWS aws = AWS.getInstance();
    private final String localAppId;
    private final String inputFileName;
    private final String inputIndex;
    private final Map<String, Review> requestReviews;
    private final String bucketName;
    private int tasksSent = 0;
    private int tasksCompleted = 0;

    public ManagerTask(String localAppId, String inputFileName, String inputIndex, Map<String, Review> requestReviews, String bucketName) {
        this.localAppId = localAppId;
        this.inputFileName = inputFileName;
        this.inputIndex = inputIndex;
        this.requestReviews = requestReviews;
        this.bucketName = bucketName;
    }

    @Override
    public void run() {
        System.out.printf("[DEBUG] ManagerTask started for localAppId %s\n", localAppId);

        sendTasksToWorkers();

        System.out.println("[DEBUG] Waiting for tasks to be completed");

        receiveTasksFromWorkers();

        System.out.printf("[DEBUG] ManagerTask finished for localAppId %s\n", localAppId);
    }

    public void sendTasksToWorkers() {
        String managerToWorkerQueueUrl = aws.sqs.getQueueUrl(MANAGER_TO_WORKER_QUEUE_NAME + "::" + localAppId);

        for (Map.Entry<String, Review> entry : requestReviews.entrySet()) {
            String reviewId = entry.getKey();
            String reviewText = entry.getValue().getText();

            String sentimentTask = String.join("::",
                    localAppId, inputFileName, inputIndex, reviewId, reviewText, SENTIMENT_ANALYSIS_TASK);
            String entityTask = String.join("::",
                    localAppId, inputFileName, inputIndex, reviewId, reviewText, ENTITY_RECOGNITION_TASK);

            aws.sqs.sendMessage(managerToWorkerQueueUrl, sentimentTask);
            aws.sqs.sendMessage(managerToWorkerQueueUrl, entityTask);

            tasksSent += 2;
        }
    }

    public void receiveTasksFromWorkers() {
        String workerToManagerQueueUrl = aws.sqs.getQueueUrl(WORKER_TO_MANAGER_QUEUE_NAME + "::" + localAppId);

        List<String> responses = aws.sqs.receiveMessages(workerToManagerQueueUrl)
                .stream().map(Message::body).toList();

        while (tasksCompleted < tasksSent) {
            for (String response : responses) {
                String[] responseContent = response.split("::");
                String localAppId = responseContent[0], inputIndex = responseContent[1], reviewId = responseContent[2],
                        taskType = responseContent[3], taskResult = responseContent[4];

                if (taskType.equals(SENTIMENT_ANALYSIS_TASK)) {
                    requestReviews.get(taskId).setSentiment(taskResult);
                } else if (taskType.equals(ENTITY_RECOGNITION_TASK)) {
                    requestReviews.get(taskId).setEntities(taskResult);
                }

                aws.sqs.deleteMessage(workerToManagerQueueUrl, message);
                tasksCompleted++;
            }
        }
    }
}