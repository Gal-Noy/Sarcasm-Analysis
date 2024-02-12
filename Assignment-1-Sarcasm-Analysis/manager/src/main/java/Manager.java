import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static final ManagerEnv env = ManagerEnv.getInstance();
    private static final Logger logger = LogManager.getLogger(Manager.class);
    private static final AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        logger.info("Manager started");

        aws.sqs.createQueueIfNotExist(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME);
        aws.sqs.createQueueIfNotExist(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME);

        handleRequestsFromLocalApps(aws.sqs.getQueueUrl(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME));

        waitForExecutorToFinish(); // Wait for all tasks to finish summary responses

        aws.ec2.terminateAllWorkers();

        aws.sqs.deleteAllQueues();
        aws.s3.emptyS3Bucket(AWSConfig.BUCKET_NAME);
        aws.s3.deleteS3Bucket(AWSConfig.BUCKET_NAME);

        aws.ec2.terminateManager();

        logger.info("Manager finished");
    }

    private static void handleRequestsFromLocalApps(String localToManagerQueueUrl) {
        while (!env.isTerminated) {
            try {
                logger.info("Polling requests from " + AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME);

                List<Message> requests = aws.sqs.receiveMessages(localToManagerQueueUrl); // long polling
                int requestsSize = requests.size();

                logger.info("Polled " + requestsSize + " requests");

                if (requestsSize > 0) {
                    handleQueueTasks(requests, localToManagerQueueUrl);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    private static void handleQueueTasks(List<Message> requests, String queueUrl) throws IOException {
        for (Message request : requests) {
            String requestBody = request.body();
            logger.info("Received request: " + requestBody);

            // <local_app_id>::<task_type>::<input_file>::<input_index>::<reviews_per_worker>
            String[] requestContent = requestBody.split(AWSConfig.MESSAGE_DELIMITER);
            String localAppId = requestContent[0], requestType = requestContent[1];

            if (requestType.equals(AWSConfig.TERMINATE_TASK)) {
                logger.info("Termination request received from local app " + localAppId);
                env.isTerminated = true;
                env.terminatingLocalAppId = localAppId;
                aws.sqs.deleteMessage(queueUrl, request);
                // Continue to serve the local app that requested termination
                continue;
            }

            String inputFileName = requestContent[2], inputIndex = requestContent[3];
            int reviewsPerWorker = Integer.parseInt(requestContent[4]);

            if (env.isTerminated && !localAppId.equals(env.terminatingLocalAppId)) {
                logger.info("Ignoring request from local app " + localAppId + " because manager is terminating");
                aws.sqs.deleteMessage(queueUrl, request);

                // <local_app_id>::error::<>::<input_index>::<error_message>
                aws.sqs.sendMessage(AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME,
                        String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, AWSConfig.RESPONSE_STATUS_ERROR,
                                "", inputIndex, "Manager is terminating"));
                continue;
            }

            Map<String, Review> requestReviews = getRequestReviews(inputFileName, localAppId);
            if (requestReviews == null) {
                logger.error("Error parsing input file " + inputFileName);
                aws.sqs.deleteMessage(queueUrl, request);

                // <local_app_id>::error::<>::<input_index>::<error_message>
                aws.sqs.sendMessage(AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME,
                        String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, AWSConfig.RESPONSE_STATUS_ERROR,
                                "", inputIndex, "Error parsing input file " + inputFileName));
                continue;
            }

            logger.info("Parsed " + requestReviews.size() + " reviews from input file " + inputFileName);

            int workersNeeded = (int) Math.ceil((double) 2 * requestReviews.size() / reviewsPerWorker);
            int workersCreated = env.assignWorkers(workersNeeded);

            // Task for each input file, to send tasks to workers, receive responses and handle summary
            env.executor.execute(new ManagerTask(
                    localAppId,
                    inputIndex,
                    requestReviews,
                    workersCreated));

            // No longer needed
            aws.s3.deleteObjectFromS3(AWSConfig.BUCKET_NAME,
                    localAppId + AWSConfig.BUCKET_KEY_DELIMITER + inputFileName);
            aws.sqs.deleteMessage(queueUrl, request);
        }
    }

    private static Map<String, Review> getRequestReviews(String inputFileName, String localAppId) throws IOException {
        InputStream inputFile = aws.s3.downloadFileFromS3(AWSConfig.BUCKET_NAME,
                localAppId + AWSConfig.BUCKET_KEY_DELIMITER + inputFileName);
        if (inputFile == null) {
            logger.error("Input file not found: " + inputFileName);
            return null;
        }
        return RequestParser.parseRequest(inputFile);
    }

    private static void waitForExecutorToFinish() {
        env.executor.shutdown();
        while (true) {
            try {
                if (env.executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.info("Executor finished");
                    break;
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

}

