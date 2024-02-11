import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import analysis.SentimentAnalysisHandler;
import analysis.NamedEntityRecognitionHandler;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public class Worker {
    private static final Logger logger = LogManager.getLogger(Worker.class);
    private static final AWS aws = new AWS(logger);
    private static final SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    private static final NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public static void main(String[] args) {
        logger.info("Worker started");
        handleTasksFromManager();
        logger.info("Worker finished");
    }

    private static void handleTasksFromManager() {
        while (true) {
            List<String> managerToWorkerQueues = aws.sqs.getAllQueuesByPrefix(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME);
            int numberOfQueues = managerToWorkerQueues.size();

            logger.info("Polling tasks from " + numberOfQueues + " manager queues");

            for (String queueUrl : managerToWorkerQueues) {
                String localAppId = aws.sqs.getLocalAppNameFromQueueUrl(queueUrl);
                try {
                    List<Message> tasks = aws.sqs.receiveMessages(queueUrl);
                    logger.info("Received " + tasks.size() + " tasks from " + localAppId);
                    if (!tasks.isEmpty()) {
                        receiveAndResponseTasks(tasks, queueUrl, localAppId);
                    }
                } catch (Exception e) {
                    continue; // The queue was deleted by the manager due to local app termination
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private static void receiveAndResponseTasks(List<Message> tasks, String queueUrl, String localAppId) {
        for (Message task : tasks) {
            try {
                String response = getTaskResponse(task.body());
                aws.sqs.sendMessage(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId, response);
            } catch (RuntimeException e) {
                logger.error(e.getMessage());
            } finally {
                aws.sqs.deleteMessage(queueUrl, task);
            }
        }
    }
    private static String getTaskResponse(String taskBody) {
        // <local_app_id>::<input_index>::<review_id>::<review_text>::<task_type>
        String[] taskContent = taskBody.split(AWSConfig.MESSAGE_DELIMITER);
        String localAppId = taskContent[0], inputIndex = taskContent[1],
                reviewId = taskContent[2], reviewText = taskContent[3], taskType = taskContent[4];

        logger.info("Received task: " + taskBody);

        // <local_app_id>::<input_index>::<review_id>::<task_type>::<task_result>
        String response = String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, inputIndex, reviewId, taskType, "");

        if (taskType.equals(AWSConfig.SENTIMENT_ANALYSIS_TASK)) {
            String sentiment = String.valueOf(sentimentAnalysisHandler.findSentiment(reviewText));
            response += sentiment; // <task_result>

            logger.info("Sentiment analysis result: " + sentiment);
        }

        if (taskType.equals(AWSConfig.ENTITY_RECOGNITION_TASK)) {
            String entities = String.join(", ", namedEntityRecognitionHandler.findEntities(reviewText));
            response += entities; // <task_result>

            logger.info("Named entity recognition result: " + entities);
        }

        return response;
    }
}
