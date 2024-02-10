import software.amazon.awssdk.services.sqs.model.Message;

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

    public static void main(String[] args) {
        System.out.println("[DEBUG] Manager started");

        ManagerEnv env = new ManagerEnv();

        while (!env.isTerminated) {
            try {
                pollTasksFromLocalApps(env);
            } catch (Exception e) {
                System.err.println("[ERROR] " + e.getMessage());
            }
        }

        // Handle termination
        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);

        aws.sqs.deleteAllManagerToWorkerQueues();
        aws.sqs.deleteAllWorkerToManagerQueues();

        aws.ec2.terminateManager();

        System.out.println("[DEBUG] Manager finished");
    }

    private static void pollTasksFromLocalApps(ManagerEnv env) throws Exception {
        List<String> localToManagerQueues = aws.sqs.getAllQueuesByPrefix(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME);

        for (String queueUrl : localToManagerQueues) {
            if (!env.polledQueues.contains(queueUrl)) {
                env.polledQueues.add(queueUrl);
            }
        }

        int numberOfQueues = env.polledQueues.size();
        System.out.println("[DEBUG] Polling from " + numberOfQueues + " local app queues");
        env.executor.setCorePoolSize(10 * numberOfQueues); // For maximum 10 input files per local app

        List<Future<?>> pendingQueuePollers = new ArrayList<>();

        for (String queueUrl : env.polledQueues) {
            Future<?> future = env.executor.submit(() -> {
                while (!env.isTerminated) { // For each local app queue, there is a thread long polling for tasks
                    try {
                        List<Message> requests = aws.sqs.receiveMessages(queueUrl); // long polling
                        System.out.println("[DEBUG] Received " + requests.size() + " requests from queue " + queueUrlToName(queueUrl));
                        handleQueueTasks(env, requests, queueUrl);
                    } catch (IOException e) {
                        System.err.println("[ERROR] " + e.getMessage());
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
            System.out.println("[DEBUG] Received request " + requestBody);

            // <local_app_id>::<task_type>::<input_file>::<input_index>::<reviews_per_worker>
            String[] requestContent = requestBody.split("::");
            String localAppId = requestContent[0], requestType = requestContent[1];

            if (requestType.equals(AWSConfig.TERMINATE_TASK)) {
                System.out.println("[DEBUG] Received terminate request from local app " + localAppId);

                env.isTerminated = true;
                aws.sqs.deleteMessage(queueUrl, request);

                // Continue to serve the local app that requested termination
                continue;
            }

            String inputFileName = requestContent[2], inputIndex = requestContent[3];
            int reviewsPerWorker = Integer.parseInt(requestContent[4]);

            Map<String, Review> requestReviews;

            InputStream inputFile = aws.s3.downloadFileFromS3(
//                        AWSConfig.BUCKET_NAME + "-" + localAppId, TODO: Uncomment this line
                    AWSConfig.BUCKET_NAME, // TODO: Delete this line
                    inputFileName);
            if (inputFile == null) {
                throw new RuntimeException("Input file not found");
            }
            requestReviews = RequestParser.parseRequest(inputFile);

//            assignWorkers(env, (int) Math.ceil((double) requestReviews.size() / reviewsPerWorker));

//                aws.sqs.createQueue(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + "-" + localAppId); TODO: Uncomment this line
//                aws.sqs.createQueue(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + "-" + localAppId); TODO: Uncomment this line

            // Task for each input file, to send tasks to workers and receive responses
            env.executor.execute(new ManagerTask(
                    localAppId,
                    inputIndex,
                    requestReviews,
//                        AWSConfig.BUCKET_NAME + "-" + localAppId TODO: Uncomment this line
                    AWSConfig.BUCKET_NAME // TODO: Delete this line
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

        aws.ec2.terminateAllWorkers();
    }

    private static void assignWorkers(ManagerEnv env, int workersNeeded) {
        System.out.println("[DEBUG] Assigning workers");
        int activeWorkers = aws.ec2.countActiveWorkers();
        int remainingWorkersCapacity = 18 - activeWorkers;
        if (remainingWorkersCapacity > 0) {
            int workersToCreate = Math.min(workersNeeded, remainingWorkersCapacity);
            env.workers += workersToCreate;
            for (int i = 0; i < workersToCreate; i++) {
                aws.ec2.createWorkerInstance();
            }
        }
        System.out.println("[DEBUG] Workers assigned");
    }

    private static void waitForExecutorToFinish(ThreadPoolExecutor executor) {
        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // TODO: Delete this method
    private static String queueUrlToName(String queueUrl) {
        return queueUrl.split("/")[4].split("-")[0];
    }

}

