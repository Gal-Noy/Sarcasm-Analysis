import software.amazon.awssdk.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static final AWS aws = AWS.getInstance();
    private static final Logger logger = LoggerFactory.getLogger(Manager.class);

    public static void main(String[] args) {
        logger.info("[INFO] Manager started");

        ManagerEnv env = new ManagerEnv();

        while (!env.isTerminated) {
            try {
                pollTasksFromLocalApps(env);
            } catch (Exception e) {
                logger.error("[ERROR] " + e.getMessage());
            }
        }

        // Handle termination
        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);

        aws.ec2.terminateManager();

        logger.info("[INFO] Manager finished");
    }

    private static void pollTasksFromLocalApps(ManagerEnv env) {
        List<String> localToManagerQueues = aws.sqs.getAllQueuesByPrefix(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME);

        for (String queueUrl : localToManagerQueues) {
            if (!env.polledQueues.contains(queueUrl)) {
                env.polledQueues.add(queueUrl);

                String localAppId = aws.sqs.getLocalAppNameFromQueueUrl(queueUrl);
                aws.sqs.createQueue(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId);
                aws.sqs.createQueue(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId);
            }
        }

        int numberOfQueues = env.polledQueues.size();
        env.executor.setCorePoolSize(10 * numberOfQueues); // For maximum 10 input files per local app

        logger.info("[INFO] Polling tasks from " + numberOfQueues + " local app queues");

        List<Future<?>> pendingQueuePollers = new ArrayList<>();

        for (String queueUrl : env.polledQueues) {
            String localAppId = aws.sqs.getLocalAppNameFromQueueUrl(queueUrl);

            Future<?> future = env.executor.submit(() -> {
                while (!env.isTerminated) { // For each local app queue, there is a thread long polling for tasks
                    try {
                        List<Message> requests = aws.sqs.receiveMessages(queueUrl); // long polling
                        logger.info("[INFO] Received " + requests.size() + " requests from local app " + localAppId);
                        if (!requests.isEmpty()) {
                            handleQueueTasks(env, requests, queueUrl);
                        }
                    } catch (Exception e) {
                        // Queue is deleted when local app terminates
                        env.polledQueues.remove(queueUrl);
                        aws.sqs.deleteQueue(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId);
                        aws.sqs.deleteQueue(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId);
                        break;
                    }
                }
            });

            pendingQueuePollers.add(future);
        }

        // After termination request and all tasks are handled for the terminating local app
        handleTermination(env, pendingQueuePollers);
    }

    private static void handleQueueTasks(ManagerEnv env, List<Message> requests, String queueUrl) throws IOException {
        for (Message request : requests) {
            String requestBody = request.body();
            logger.info("[INFO] Received request: " + requestBody);

            // <local_app_id>::<task_type>::<input_file>::<input_index>::<reviews_per_worker>
            String[] requestContent = requestBody.split(AWSConfig.MESSAGE_DELIMITER);
            String localAppId = requestContent[0], requestType = requestContent[1];

            if (requestType.equals(AWSConfig.TERMINATE_TASK)) {
                logger.info("[INFO] Termination request received from local app " + localAppId);

                env.isTerminated = true;
                aws.sqs.deleteMessage(queueUrl, request);

                // Continue to serve the local app that requested termination
                continue;
            }

            String inputFileName = requestContent[2], inputIndex = requestContent[3];
            int reviewsPerWorker = Integer.parseInt(requestContent[4]);

            Map<String, Review> requestReviews;

            InputStream inputFile = aws.s3.downloadFileFromS3(
                    AWSConfig.BUCKET_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId,
                    inputFileName);
            if (inputFile == null) {
                logger.error("[ERROR] Input file not found: " + inputFileName);
                aws.sqs.deleteMessage(queueUrl, request);
                continue;
            }
            requestReviews = RequestParser.parseRequest(inputFile);
            logger.info("[INFO] Parsed " + requestReviews.size() + " reviews from input file " + inputFileName);

//            assignWorkers(env, (int) Math.ceil((double) requestReviews.size() / reviewsPerWorker));

            // Task for each input file, to send tasks to workers and receive responses
            env.executor.execute(new ManagerTask(
                    localAppId,
                    inputIndex,
                    requestReviews,
                    AWSConfig.BUCKET_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId,
                    logger
            ));

            aws.sqs.deleteMessage(queueUrl, request);
        }
    }

    private static void handleTermination(ManagerEnv env, List<Future<?>> pendingQueuePollers) {
        // Main thread waits for all queue pollers to finish
        for (Future<?> pendingQueuePoller : pendingQueuePollers) {
            try {
                pendingQueuePoller.get();
            } catch (Exception e) {
                System.err.println("[ERROR] " + e.getMessage());
            }
        }

        logger.info("[INFO] All local app queues are removed");

        aws.ec2.terminateAllWorkers();
    }

    private static void assignWorkers(ManagerEnv env, int workersNeeded) {
        logger.info("[INFO] Assigning workers");

        int activeWorkers = aws.ec2.countActiveWorkers();
        int remainingWorkersCapacity = 18 - activeWorkers;
        if (remainingWorkersCapacity > 0) {
            int workersToCreate = Math.min(workersNeeded, remainingWorkersCapacity);
            env.workers += workersToCreate;
            for (int i = 0; i < workersToCreate; i++) {
                aws.ec2.createWorkerInstance();
            }
        }

        logger.info("[INFO] Workers assigned: " + env.workers);
    }

    private static void waitForExecutorToFinish(ThreadPoolExecutor executor) {
        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.info("[INFO] Executor terminated");
                    break;
                }
            } catch (InterruptedException e) {
                logger.error("[ERROR] " + e.getMessage());
            }
        }
    }

}

