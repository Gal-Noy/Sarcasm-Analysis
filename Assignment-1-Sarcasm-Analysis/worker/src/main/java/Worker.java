import java.util.List;

import analysis.SentimentAnalysisHandler;
import analysis.NamedEntityRecognitionHandler;
import software.amazon.awssdk.services.sqs.model.Message;


public class Worker {
    private static final AWS aws = AWS.getInstance();
    private static final SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    private static final NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public static void main(String[] args) {
        System.out.println("[DEBUG] Worker started");
        handleTasksFromManager();
        System.out.println("[DEBUG] Worker finished");
    }

    private static void handleTasksFromManager() {
        int tasksCompleted = 0;
        while (true) {
            List<String> managerToWorkerQueues = aws.sqs.getAllQueuesByPrefix(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME);
            for (String queueUrl : managerToWorkerQueues) {
                List<Message> tasks = aws.sqs.receiveMessages(queueUrl); // long polling
                System.out.println("[DEBUG] Received " + tasks.size() + " tasks from queue");
                for (Message task : tasks) {
                    try {
                        String response = getTaskResponse(task.body());

                        aws.sqs.sendMessage(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME, response); // TODO: Delete this line
//            aws.sqs.sendMessage(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + "-" + localAppId, response); TODO: Uncomment this line

                    } catch (RuntimeException e) {
                        System.err.println("[ERROR] " + e.getMessage());
                    } finally {
                        aws.sqs.deleteMessage(queueUrl, task);
                        tasksCompleted++;
                        System.out.println("[DEBUG] Completed " + tasksCompleted + " tasks");
                    }
                }
            }
        }
    }

    private static String getTaskResponse(String taskBody) {
        // <local_app_id>::<input_index>::<review_id>::<review_text>::<task_type>
        String[] taskContent = taskBody.split("::");
        String localAppId = taskContent[0], inputIndex = taskContent[1],
                reviewId = taskContent[2], reviewText = taskContent[3], taskType = taskContent[4];

        // <local_app_id>::<input_index>::<review_id>::<task_type>::<task_result>
        String response = String.join("::", localAppId, inputIndex, reviewId, taskType, "");

        if (taskType.equals(AWSConfig.SENTIMENT_ANALYSIS_TASK)) {
            String sentiment = String.valueOf(sentimentAnalysisHandler.findSentiment(reviewText));
            response += sentiment; // <task_result>
        }

        if (taskType.equals(AWSConfig.ENTITY_RECOGNITION_TASK)) {
            String entities = String.join(";", namedEntityRecognitionHandler.findEntities(reviewText));
            response += entities; // <task_result>
        }

        return response;
    }
}
