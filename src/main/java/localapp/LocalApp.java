package localapp;

import aws.AWS;
import static aws.AWSConfig.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import software.amazon.awssdk.services.sqs.model.Message;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LocalApp {
    private static final AWS aws = new AWS();
    private static final String localAppId = UUID.randomUUID().toString().replace(DEFAULT_DELIMITER, "").substring(16);
    private static final Logger logger = LogManager.getLogger(LocalApp.class);

    public static void main(String[] args) {
        int argsLen = args.length;
        if (argsLen == 1 && args[0].equals(TERMINATE_TASK)) {
            terminateManager();
            return;
        }
        if (argsLen < 3 || (argsLen % 2 != 0 && args[argsLen - 1].equals(TERMINATE_TASK))) {
            logger.error("Usage: sarcasm-analysis <input_file1> ... <input_fileN> <output_file1> ... <output_fileN> <n> [terminate]");
            return;
        }

        logger.info("LocalApp started with id " + localAppId);

        LocalAppEnv env = new LocalAppEnv(args);
        logger.info("LocalAppEnv created");

        String localToManagerQueueUrl, managerToLocalQueueUrl;
        try {
            aws.s3.createS3BucketIfNotExists(BUCKET_NAME);
            localToManagerQueueUrl = aws.sqs.createQueueIfNotExist(LOCAL_TO_MANAGER_QUEUE_NAME);
            managerToLocalQueueUrl = aws.sqs.createQueueIfNotExist(MANAGER_TO_LOCAL_QUEUE_NAME);
        } catch (Exception e) {
            logger.error("Error creating S3 bucket or SQS queues: " + e.getMessage());
            return;
        }

        sendTasksToManager(env, localToManagerQueueUrl);

        startManager();

        receiveResponsesFromManager(env, managerToLocalQueueUrl);

        if (env.terminate) {
            terminateManager();
        }

        waitForExecutorToFinish(env.executor);

        // Delete S3 folder
        aws.s3.deleteObjectFromS3(BUCKET_NAME, localAppId + BUCKET_KEY_DELIMITER);

        logger.info("LocalApp finished");
    }

    private static void sendTasksToManager(LocalAppEnv env, String localToManagerQueueUrl) {
        for (int inputIndex = 0; inputIndex < env.inputFilesPaths.length; inputIndex++) {
            File inputFile = new File(env.inputFilesPaths[inputIndex]);
            aws.s3.uploadFileToS3(BUCKET_NAME, localAppId + BUCKET_KEY_DELIMITER + inputFile.getName(), inputFile);
            // <local_app_id>::analyze::<input_file>::<input_index>::<reviews_per_worker>
            aws.sqs.sendMessage(localToManagerQueueUrl, String.join(MESSAGE_DELIMITER,
                    localAppId,
                    ANALYZE_TASK,
                    inputFile.getName(), // S3 bucket key
                    Integer.toString(inputIndex),
                    env.reviewsPerWorker + ""));

            logger.info("Sent task to manager: " + inputFile.getName() + " for inputIndex " + inputIndex + " with " + env.reviewsPerWorker + " reviews per worker");
        }
    }

    private static void receiveResponsesFromManager(LocalAppEnv env, String managerToLocalQueueUrl) {
        int filesLeftToProcess = env.numberOfFiles;
        while (filesLeftToProcess > 0) {
            logger.info("Polling for responses from manager");
            List<Message> responses = aws.sqs.receiveMessages(managerToLocalQueueUrl); // long polling
            for (Message response : responses) { // response for each input file
                String responseBody = response.body();
                // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
                String[] responseContent = responseBody.split(MESSAGE_DELIMITER, -1);
                String receivedLocalAppId = responseContent[0],
                        status = responseContent[1], // done or error
                        summaryFileName = responseContent[2],
                        inputIndex = responseContent[3];

                if (receivedLocalAppId.equals(localAppId)) {
                    logger.info("Received response from manager: " + responseBody);

                    if (status.equals(RESPONSE_STATUS_DONE)) {
                        Future<?> localAppTask = env.executor.submit(new LocalAppTask(
                                localAppId,
                                env.outputFilesPaths[Integer.parseInt(inputIndex)],
                                summaryFileName));

                        // Wait for task to finish
                        try {
                            localAppTask.get();
                        } catch (Exception e) {
                            logger.error(e.getMessage());
                        }
                    } else {
                        String errorMessage = responseContent[4];
                        logger.error("Error response from manager: " + errorMessage);
                    }

                    filesLeftToProcess--;
                    aws.sqs.deleteMessage(managerToLocalQueueUrl, response);
                } else {
                    // Put back in queue
                    logger.info("Putting back irrelevant response in managerToLocal queue");
                    aws.sqs.changeMessageVisibility(managerToLocalQueueUrl, response, RETURN_TASK_TIME);
                }
            }
        }

    }

    private static void waitForExecutorToFinish(ThreadPoolExecutor executor) {
        logger.info("Waiting for executor to finish");
        executor.shutdown();
        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.info("Executor finished");
                    break;
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }
    private static void startManager() {
        try {
            aws.ec2.runManager();
        }
        catch (Exception e) {
            logger.error("Error running manager: " + e.getMessage());
            System.exit(1);
        }
    }
    private static void terminateManager() {
        logger.info("Sending terminate message to manager");
        try {
            String localToManagerQueueUrl = aws.sqs.getQueueUrl(LOCAL_TO_MANAGER_QUEUE_NAME);
            aws.sqs.sendMessage(localToManagerQueueUrl, String.join(MESSAGE_DELIMITER,
                    localAppId, TERMINATE_TASK));
        } catch (Exception e) {
            logger.error("Error sending terminate message to manager: " + e.getMessage());
        }
    }

}


