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
        int tasksCompleted = 0; // TODO: Delete this line
        while (true) {
            List<String> managerToWorkerQueues = aws.sqs.getAllManagerToWorkerQueues();
            for (String queueUrl : managerToWorkerQueues) {
                List<Message> tasks = aws.sqs.receiveMessages(queueUrl); // long polling
                for (Message task : tasks) {
                    try {
                        String taskBody = task.body();
                        // <local_app_id>::<input_index>::<review_id>::<review_text>
                        String[] taskContent = taskBody.split("::");
                        String localAppId = taskContent[0], inputIndex = taskContent[1],
                                reviewId = taskContent[2], reviewText = taskContent[3];

                        String sentiment = String.valueOf(sentimentAnalysisHandler.findSentiment(reviewText));
                        String entities = String.join(";", namedEntityRecognitionHandler.findEntities(reviewText));

                        // <local_app_id>::<sentiment>::<entities>::<input_index>::<review_id>
                        String response = String.join("::", localAppId, sentiment, entities, inputIndex, reviewId);

                        aws.sqs.sendMessage(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME, response); // TODO: Delete this line
//            aws.sqs.sendMessage(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + "-" + localAppId, response); TODO: Uncomment this line
                    } catch (RuntimeException e) {
                        System.err.println("[ERROR] " + e.getMessage());
                    } finally {
                        tasksCompleted++; // TODO: Delete this line
                        System.out.println("[DEBUG] Completed " + tasksCompleted + " tasks"); // TODO: Delete this line
                        aws.sqs.deleteMessage(queueUrl, task);

                    }
                }
            }
        }
    }
}
