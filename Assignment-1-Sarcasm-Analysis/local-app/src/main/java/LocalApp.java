
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LocalApp {
    private static final String localAppId = UUID.randomUUID().toString().replace(AWSConfig.DEFAULT_DELIMITER, "").substring(0, 16);
    private static final Logger logger = LogManager.getLogger(LocalApp.class);
    private static final AWS aws = new AWS(logger);

    public static void main(String[] args) {
        int argsLen = args.length;
        if (argsLen < 3 || (argsLen % 2 != 0 && args[argsLen - 1].equals(AWSConfig.TERMINATE_TASK))
                || Integer.parseInt(args[argsLen - 1]) == 0) {
            logger.error("Usage: LocalApp <input_file1> ... <input_fileN> <output_file1> ... <output_fileN> <n> [terminate]");
            return;
        }

        logger.info("LocalApp started with id " + localAppId);

        LocalAppEnv env = new LocalAppEnv(args);

        logger.info("LocalAppEnv created");

        String bucketName = AWSConfig.BUCKET_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId;
        aws.s3.createS3BucketIfNotExists(bucketName);

        String localToManagerQueueName = AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId;
        String managerToLocalQueueName = AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + AWSConfig.DEFAULT_DELIMITER + localAppId;

        String localToManagerQueueUrl = aws.sqs.createQueue(localToManagerQueueName);
        String managerToLocalQueueUrl = aws.sqs.createQueue(managerToLocalQueueName);

        sendTasksToManager(env, bucketName, localToManagerQueueUrl);

        //        aws.ec2.runManager(); TODO: Uncomment this line

        receiveResponsesFromManager(env, bucketName, managerToLocalQueueUrl);

        if (env.terminate) {
            logger.info("Sending terminate message to manager");

            // <local_app_id>::terminate
            aws.sqs.sendMessage(localToManagerQueueUrl,
                    String.join(AWSConfig.MESSAGE_DELIMITER, localAppId, AWSConfig.TERMINATE_TASK));
        }

        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);

        aws.sqs.deleteQueue(managerToLocalQueueUrl);
        aws.sqs.deleteQueue(localToManagerQueueUrl);

        aws.s3.emptyS3Bucket(bucketName);
        aws.s3.deleteS3Bucket(bucketName);

        logger.info("LocalApp finished");
    }

    private static void sendTasksToManager(LocalAppEnv env, String bucketName, String localToManagerQueueUrl) {
        for (int inputIndex = 0; inputIndex < env.inputFilesPaths.length; inputIndex++) {
            File inputFile = new File(env.inputFilesPaths[inputIndex]);
            aws.s3.uploadFileToS3(bucketName, inputFile);
            // <local_app_id>::analyze::<input_file>::<input_index>::<reviews_per_worker>
            aws.sqs.sendMessage(localToManagerQueueUrl, String.join(AWSConfig.MESSAGE_DELIMITER,
                    localAppId,
                    AWSConfig.ANALYZE_TASK,
                    inputFile.getName(), // S3 bucket key
                    Integer.toString(inputIndex),
                    env.reviewsPerWorker + ""));

            logger.info("Sent task to manager: " + inputFile.getName() + " for inputIndex " + inputIndex + " with " + env.reviewsPerWorker + " reviews per worker");
        }
    }

    private static void receiveResponsesFromManager(LocalAppEnv env, String bucketName, String managerToLocalQueueUrl) {
        int filesLeftToProcess = env.numberOfFiles;
        while (filesLeftToProcess > 0) {
            logger.info("Polling for responses from manager");
            List<Message> responses = aws.sqs.receiveMessages(managerToLocalQueueUrl);
            for (Message response : responses) { // response for each input file
                String responseBody = response.body();
                // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
                String[] responseContent = responseBody.split("::");
                String receivedLocalAppId = responseContent[0],
                        status = responseContent[1], // done or error
                        summaryFileName = responseContent[2],
                        inputIndex = responseContent[3];

                if (receivedLocalAppId.equals(localAppId)) {
                    logger.info("Received response from manager: " + responseBody);

                    if (status.equals(AWSConfig.RESPONSE_STATUS_DONE)) {
                        Future<?> localAppTask = env.executor.submit(new LocalAppTask(
                                env.outputFilesPaths[Integer.parseInt(inputIndex)],
                                summaryFileName, // S3 bucket key
                                bucketName,
                                logger));

                        // Wait for task to finish
                        try {
                            localAppTask.get();
                        } catch (Exception e) {
                            logger.error(e.getMessage());
                        }

                        aws.s3.deleteFileFromS3(bucketName, summaryFileName);
                        filesLeftToProcess--;
                    }
                    aws.sqs.deleteMessage(managerToLocalQueueUrl, response);
                }
            }
        }

    }

    private static void waitForExecutorToFinish(ThreadPoolExecutor executor) {
        logger.info("Waiting for executor to finish");

        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

}


