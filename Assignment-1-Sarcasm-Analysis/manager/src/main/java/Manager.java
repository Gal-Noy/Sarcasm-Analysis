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

import static aws.AWSConfig.BUCKET_NAME;
import static aws.AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME;

public class Manager {
    public static void main(String[] args) {
        System.out.println("Manager started");
        boolean isTerminated = false;
        int workers = 0, activeWorkers = 0;
        String terminatingLocalApp = null;
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        AWS aws = AWS.getInstance();

        while (!isTerminated) {
            List<String> localToManagerQueues = aws.sqs.getAllLocalToManagerQueues();
            for (String queueUrl : localToManagerQueues) {
                List<String> requests = aws.sqs.receiveMessages(queueUrl)
                        .stream().map(Message::body).toList();
                for (String request : requests) {
                    // request format: <localAppId>::<requestType>::<inputFileName>::<inputIndex>::<reviewsPerWorker>
                    String[] requestContent = request.split("::");
                    String localAppId = requestContent[0], requestType = requestContent[1];

                    if (requestType.equals("terminate")) {
                        executor.shutdown();
                        waitForExecutorToFinish(executor);
                        isTerminated = true;
                        aws.sqs.deleteMessage(queueUrl, request);
//                        terminatingLocalApp = localAppId;
//                        aws.sqs.handleTerminationByLocalApp(localAppId);
                        break;
                    }

                    String inputFileName = requestContent[2], inputIndex = requestContent[3];
                    int reviewsPerWorker = Integer.parseInt(requestContent[4]);

                    InputStream inputFile = aws.s3.downloadFileFromS3(
                            AWSConfig.BUCKET_NAME + "::" + localAppId, inputFileName);
                    Map<String, Review> requestReviews = RequestParser.parseRequest(inputFile);
                    int totalReviews = requestReviews.size();

                    activeWorkers = aws.ec2.countActiveWorkers();
                    int remainingWorkersCapacity = 18 - activeWorkers;
                    if (remainingWorkersCapacity > 0) {
                        int workersNeeded = (int) Math.ceil((double) totalReviews / reviewsPerWorker);
                        int workersToCreate = Math.min(workersNeeded, remainingWorkersCapacity);
                        workers += workersToCreate;
                        for (int i = 0; i < workersToCreate; i++) {
                            aws.ec2.createWorkerInstance();
                            activeWorkers++;
                        }
                    }

                    executor.execute(new ManagerTask(
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

