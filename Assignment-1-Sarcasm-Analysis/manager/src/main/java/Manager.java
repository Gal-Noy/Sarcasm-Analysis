import aws.AWS;
import aws.AWSConfig;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.thirdparty.jackson.core.JsonParser;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static aws.AWSConfig.*;

public class Manager {
    private static final AWS aws = AWS.getInstance();
    private int workers = 0;
    private int activeWorkers = 0;

    public static void main(String[] args) {
        System.out.println("[DEBUG] Manager started");

        ManagerEnv env = new ManagerEnv();

        while (!env.isTerminated) {
            handleTasksFromLocalApps(env);
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

        // wait for all workers to finish

        // create response messages for the jobs if needed

        // terminates the manager instance

    }

    private static void handleTasksFromLocalApps(ManagerEnv env) {
        List<String> localToManagerQueues = aws.sqs.getAllLocalToManagerQueues();
        for (String queueUrl : localToManagerQueues) {
            List<String> requests = aws.sqs.receiveMessages(queueUrl)
                    .stream().map(Message::body).toList();
            for (String request : requests) {
                // <local_app_id>::<task_type>::<input_file>::<input_index>::<reviews_per_worker>
                String[] requestContent = request.split("::");
                String localAppId = requestContent[0], requestType = requestContent[1];

                if (requestType.equals(TERMINATE_TASK)) {
                    handleTerminateRequest(env, localAppId, queueUrl, request);
                    break;
                }

                String inputFileName = requestContent[2], inputIndex = requestContent[3];
                int reviewsPerWorker = Integer.parseInt(requestContent[4]);

                InputStream inputFile = aws.s3.downloadFileFromS3(
                        AWSConfig.BUCKET_NAME + "::" + localAppId, inputFileName);
                Map<String, Review> requestReviews = RequestParser.parseRequest(inputFile);

                assignWorkers(env, (int) Math.ceil((double) requestReviews.size() / reviewsPerWorker));

                env.executor.execute(new ManagerTask(
                        localAppId,
                        inputFileName,
                        inputIndex,
                        requestReviews,
                        BUCKET_NAME + "::" + localAppId
                ));

                aws.sqs.deleteMessage(queueUrl, request);
            }
        }
    }

    private static void handleTerminateRequest(ManagerEnv env, String localAppId, String queueUrl, String request) {
        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);
        env.isTerminated = true;
        aws.sqs.deleteMessage(queueUrl, request);
//                        terminatingLocalApp = localAppId;
//                        aws.sqs.handleTerminationByLocalApp(localAppId);
    }

    private static void assignWorkers(ManagerEnv env, int workersNeeded) {
        int activeWorkers = aws.ec2.countActiveWorkers();
        int remainingWorkersCapacity = 18 - activeWorkers;
        if (remainingWorkersCapacity > 0) {
            int workersToCreate = Math.min(workersNeeded, remainingWorkersCapacity);
            env.workers += workersToCreate;
            for (int i = 0; i < workersToCreate; i++) {
                aws.ec2.createWorkerInstance();
            }
        }
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

