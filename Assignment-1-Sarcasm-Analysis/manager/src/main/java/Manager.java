import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {
    private static final AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        System.out.println("[DEBUG] Manager started");

        ManagerEnv env = new ManagerEnv();

        while (!env.isTerminated) {
            try {
                handleTasksFromLocalApps(env);
            } catch (Exception e) {
                System.err.println("[ERROR] " + e.getMessage());
            }
        }

        // Serve the local app that requested termination
//        String terminatingAppQueueUrl = aws.sqs.getQueueUrl(
//                LOCAL_TO_MANAGER_QUEUE_NAME + "::" + terminatingLocalApp);
//        String terminatingAppS3Bucket = AWSConfig.BUCKET_NAME + "::" + terminatingLocalApp;
//        List<String> remainingRequests = aws.sqs.receiveMessages(terminatingAppQueueUrl)
//                .stream().map(Message::body).toList();
//        for (String request : remainingRequests) {
//            String[] requestContent = request.split("::");
//            String requestType = requestContent[1], inputFileName = requestContent[2], inputIndex = requestContent[3];
//            int reviewsPerWorker = Integer.parseInt(requestContent[4]);
//            InputStream inputFile = aws.s3.downloadFileFromS3(terminatingAppS3Bucket, inputFileName);
//            int totalReviews = getReviewsCount(inputFile);
//            int workersNeeded = (int) Math.ceil((double) totalReviews / reviewsPerWorker);
//            workers += workersNeeded;
//            for (int i = 0; i < workersNeeded; i++) {
//
//            }
//        }

        // wait for all workers to finish and delete the queues

        // create response messages for the jobs if needed

        // terminates the manager instance

    }

    private static void handleTasksFromLocalApps(ManagerEnv env) throws Exception {
        List<String> localToManagerQueues = aws.sqs.getAllLocalToManagerQueues();
        int numberOfQueues = localToManagerQueues.size();
        env.executor.setCorePoolSize(numberOfQueues);
        System.out.println("[DEBUG] Received " + numberOfQueues + " local app queues");

        for (String queueUrl : localToManagerQueues) {
            System.out.println("[DEBUG] Looking for requests in queue");
            List<Message> requests = aws.sqs.receiveMessages(queueUrl);
            for (Message request : requests) {
                String requestBody = request.body();
                System.out.println("[DEBUG] Received request " + requestBody);
                // <local_app_id>::<task_type>::<input_file>::<input_index>::<reviews_per_worker>
                String[] requestContent = requestBody.split("::");
                String localAppId = requestContent[0], requestType = requestContent[1];

                if (requestType.equals(AWSConfig.TERMINATE_TASK)) {
                    System.out.println("[DEBUG] Received terminate request");
                    handleTerminateRequest(env, localAppId, queueUrl, request);
                    break;
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

//                assignWorkers(env, (int) Math.ceil((double) requestReviews.size() / reviewsPerWorker)); TODO: Uncomment this line

//                aws.sqs.createQueue(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME + "-" + localAppId); TODO: Uncomment this line
//                aws.sqs.createQueue(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME + "-" + localAppId); TODO: Uncomment this line

                aws.sqs.createQueue(AWSConfig.MANAGER_TO_WORKER_QUEUE_NAME); // TODO: Delete this line
                aws.sqs.createQueue(AWSConfig.WORKER_TO_MANAGER_QUEUE_NAME); // TODO: Delete this line

                env.executor.execute(new ManagerTask(
                        localAppId,
                        inputFileName,
                        inputIndex,
                        requestReviews,
//                        AWSConfig.BUCKET_NAME + "-" + localAppId TODO: Uncomment this line
                        AWSConfig.BUCKET_NAME // TODO: Delete this line
                ));
//                aws.sqs.deleteMessage(queueUrl, request);
            }
        }
    }

    private static void handleTerminateRequest(ManagerEnv env, String localAppId, String queueUrl, Message request) {
        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);
        env.isTerminated = true;
        aws.sqs.deleteMessage(queueUrl, request);
//                        terminatingLocalApp = localAppId;
//                        aws.sqs.handleTerminationByLocalApp(localAppId);
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

}

