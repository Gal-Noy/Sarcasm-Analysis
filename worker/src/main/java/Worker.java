import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import analysis.SentimentAnalysisHandler;
import analysis.NamedEntityRecognitionHandler;
import software.amazon.awssdk.services.sqs.model.Message;


public class Worker {
    private static final Logger logger = LogManager.getLogger(Worker.class);
    private static final AWS aws = AWS.getInstance();
    private static final SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    private static final NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public static void main(String[] args) {
        logger.info("Worker started");
        handleTasksFromManager();
        logger.info("Worker finished");
    }

    private static void handleTasksFromManager() {
        String managerToWorkerQueueUrl = aws.sqs.getQueueUrl(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME);
        String workerToManagerQueueUrl = aws.sqs.getQueueUrl(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME);

        while (true) {
            try {
                logger.info("Polling tasks from " + AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME);
                Message task = aws.sqs.receiveSingleMessage(managerToWorkerQueueUrl); // long polling
                if (task != null) {
                    Thread extendMessageVisibility = new Thread(new ExtendTaskVisibility(task, managerToWorkerQueueUrl));
                    extendMessageVisibility.start();
                    String response = processTask(task.body());
                    extendMessageVisibility.interrupt();
                    if (response != null) {
                        aws.sqs.sendMessage(workerToManagerQueueUrl, response);
                        aws.sqs.deleteMessage(managerToWorkerQueueUrl, task);
                    } else {
                        // Put the task back in the queue
                        aws.sqs.changeMessageVisibility(managerToWorkerQueueUrl, task, AWSConfig.RETURN_TASK_TIME);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    private static String processTask(String taskBody) {
        try {
            // <local_app_id>::<input_index>::<review_id>::<review_text>::<task_type>
            String[] taskContent = taskBody.split(AWSConfig.MESSAGE_DELIMITER);
            String localAppId = taskContent[0], inputIndex = taskContent[1],
                    reviewId = taskContent[2], reviewText = taskContent[3], taskType = taskContent[4];

            logger.info("Received task: " + taskBody + " for localAppId " + localAppId + " for inputIndex " + inputIndex);

            // <local_app_id>::<input_index>::<review_id>::<task_type>::<task_result>
            String response = String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, inputIndex, reviewId, taskType, "");

            if (taskType.equals(AWSConfig.SENTIMENT_ANALYSIS_TASK)) {
                String sentiment = String.valueOf(sentimentAnalysisHandler.findSentiment(reviewText));
                response += sentiment; // <task_result>
            }
            if (taskType.equals(AWSConfig.ENTITY_RECOGNITION_TASK)) {
                String entities = String.join(", ", namedEntityRecognitionHandler.findEntities(reviewText));
                response += entities; // <task_result>
            }

            return response;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
