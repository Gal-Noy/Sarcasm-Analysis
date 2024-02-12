import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static final ManagerEnv env = ManagerEnv.getInstance();
    private static final Logger logger = LogManager.getLogger(Manager.class);
    private static final AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        logger.info("Manager started");

        aws.sqs.createQueue(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME);
        aws.sqs.createQueue(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME);

        while (!env.isTerminated) {
            try {
                pollTasksFromLocalApps();
                Thread.sleep(5000); // Check for new local app queues every 3 seconds
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        handleTermination();
        env.executor.shutdown();
        waitForExecutorToFinish();

        aws.sqs.deleteQueue(aws.sqs.getQueueUrl(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME));
        aws.sqs.deleteQueue(aws.sqs.getQueueUrl(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME));

        aws.ec2.terminateManager();

        logger.info("Manager finished");
    }

    private static void pollTasksFromLocalApps() {
        List<String> localToManagerQueues = aws.sqs.getAllQueuesByPrefix(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME);
        logger.info("Polling tasks from " + localToManagerQueues.size() + " local app queues");

        for (String queueUrl : localToManagerQueues) {
            if (!env.polledQueues.contains(queueUrl)) {
                env.polledQueues.add(queueUrl);

                Future<?> future = env.executor.submit(() -> {
                    logger.info("Started polling from " + aws.sqs.getLocalAppNameFromQueueUrl(queueUrl));
                    while (!env.isTerminated) { // For each local app queue, there is a thread long polling for tasks
                        try {
                            List<Message> requests = aws.sqs.receiveMessages(queueUrl); // long polling
                            logger.info("Received " + requests.size() + " requests");
                            if (!requests.isEmpty() && !env.isTerminated) {
                                // In this method, another thread is created for each request (ManagerTask)
                                handleQueueTasks(requests, queueUrl);
                            }
                        } catch (IOException e) {
                            logger.error(e.getMessage());
                        } catch (Exception e) {
                            // Queue is deleted when local app terminates
//                            env.polledQueues.remove(queueUrl);
                            break;
                        }
                    }
                    logger.info("Stopped polling from " + aws.sqs.getLocalAppNameFromQueueUrl(queueUrl));
                });

                env.pendingQueuePollers.add(future);
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

                // <local_app_id>::error::<>::<input_index>
                aws.sqs.sendMessage(AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId,
                        String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, AWSConfig.RESPONSE_STATUS_ERROR, "", inputIndex));
                continue;
            }

            Map<String, Review> requestReviews = getRequestReviews(inputFileName, AWSConfig.BUCKET_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId);
            if (requestReviews == null) {
                logger.error("Error parsing input file " + inputFileName);
                aws.sqs.deleteMessage(queueUrl, request);

                // <local_app_id>::error::<>::<input_index>
                aws.sqs.sendMessage(AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId,
                        String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, AWSConfig.RESPONSE_STATUS_ERROR, "", inputIndex));
                continue;
            }

            logger.info("Parsed " + requestReviews.size() + " reviews from input file " + inputFileName);

            int workersNeeded = (int) Math.ceil((double) 2 * requestReviews.size() / reviewsPerWorker);
//            env.assignWorkers(workersNeeded);

            // Task for each input file, to send tasks to workers, receive responses and handle summary
            env.executor.execute(new ManagerTask(
                    localAppId,
                    inputIndex,
                    requestReviews,
                    AWSConfig.BUCKET_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId,
                    workersNeeded));

            aws.sqs.deleteMessage(queueUrl, request);
        }
    }

    private static void handleTermination() {
        // Main thread waits for all queue pollers to finish
        for (Future<?> pendingQueuePoller : env.pendingQueuePollers) {
            try {
                pendingQueuePoller.get();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        logger.info("All local app queues are removed");

        aws.ec2.terminateAllWorkers();
    }

    private static Map<String, Review> getRequestReviews(String inputFileName, String bucketName) throws IOException {
        InputStream inputFile = aws.s3.downloadFileFromS3(
                bucketName,
                inputFileName);
        if (inputFile == null) {
            logger.error("Input file not found: " + inputFileName);
            return null;
        }
        return RequestParser.parseRequest(inputFile);
    }

    private static void waitForExecutorToFinish() {
        while (true) {
            try {
                if (env.executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.info("Executor terminated");
                    break;
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

}

