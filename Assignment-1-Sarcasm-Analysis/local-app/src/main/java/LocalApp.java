
import software.amazon.awssdk.services.s3.model.CSVOutput;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LocalApp {
    private static final AWS aws = AWS.getInstance();
    private static final String localAppId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);

    public static void main(String[] args) {
        int argsLen = args.length;
        if (argsLen < 3 || (argsLen % 2 != 0 && args[argsLen - 1].equals("terminate"))
                || Integer.parseInt(args[argsLen - 1]) == 0) {
            System.out.println("Usage: LocalApp <input_file1> ... <input_fileN> <output_file1> ... <output_fileN> <n> [terminate]");
            return;
        }
        System.out.println("[DEBUG] LocalApp started with id " + localAppId);

        LocalAppEnv env = new LocalAppEnv(args);

//        aws.ec2.runManager(); TODO: Uncomment this line

//        String bucketName = AWSConfig.BUCKET_NAME + "-" + localAppId; TODO: Uncomment this line
        String bucketName = AWSConfig.BUCKET_NAME; // TODO: Delete this line
        aws.s3.createS3BucketIfNotExists(bucketName);

        // TODO: Uncomment these lines
//        String localToManagerQueueName = AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME + "-" + localAppId;
//        String managerToLocalQueueName = AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME + "-" + localAppId;

        // TODO: Delete these lines
        String localToManagerQueueName = AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME;
        String managerToLocalQueueName = AWSConfig.MANAGER_TO_LOCAL_QUEUE_NAME;

        String localToManagerQueueUrl = aws.sqs.createQueue(localToManagerQueueName);
        String managerToLocalQueueUrl = aws.sqs.createQueue(managerToLocalQueueName);

        sendTasksToManager(env, bucketName, localToManagerQueueUrl);

        receiveResponsesFromManager(env, bucketName, managerToLocalQueueUrl);

        if (env.terminate) {
            System.out.println("[DEBUG] Sending terminate message to manager");

            // <local_app_id>::terminate
            aws.sqs.sendMessage(localToManagerQueueUrl,
                    String.join("::", localAppId, AWSConfig.TERMINATE_TASK));
        }

        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);

        // TODO: Uncomment these 4 lines
//        aws.sqs.deleteQueue(managerToLocalQueueUrl);
//        aws.sqs.deleteQueue(localToManagerQueueUrl);

//        aws.s3.emptyS3Bucket(bucketName);
//        aws.s3.deleteS3Bucket(bucketName);

        System.out.println("[DEBUG] LocalApp finished");
    }

    private static void sendTasksToManager(LocalAppEnv env, String bucketName, String localToManagerQueueUrl) {
        for (int inputIndex = 0; inputIndex < env.inputFilesPaths.length; inputIndex++) {
            File inputFile = new File(env.inputFilesPaths[inputIndex]);
            aws.s3.uploadFileToS3(bucketName, inputFile);
            // <local_app_id>::analyze::<input_file>::<input_index>::<reviews_per_worker>
            aws.sqs.sendMessage(localToManagerQueueUrl, String.join("::",
                    localAppId,
                    AWSConfig.ANALYZE_TASK,
                    inputFile.getName(), // S3 bucket key
                    Integer.toString(inputIndex),
                    env.reviewsPerWorker + ""));

            System.out.println("[DEBUG] Sent task to manager: " + inputFile.getName());
        }
    }

    private static void receiveResponsesFromManager(LocalAppEnv env, String bucketName, String managerToLocalQueueUrl) {
        int filesLeftToProcess = env.numberOfFiles;
        while (filesLeftToProcess > 0) {
            System.out.println("[DEBUG] Receiving responses from manager");
            List<Message> responses = aws.sqs.receiveMessages(managerToLocalQueueUrl);
            for (Message response : responses) {
                String responseBody = response.body();
                // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
                String[] responseContent = responseBody.split("::");
                String receivedLocalAppId = responseContent[0],
                        status = responseContent[1], // done or error
                        summaryFileName = responseContent[2],
                        inputIndex = responseContent[3];

                if (receivedLocalAppId.equals(localAppId)) {
                    System.out.println("[DEBUG] Received response from manager: " + summaryFileName + " " + status);

                    if (status.equals(AWSConfig.RESPONSE_STATUS_DONE)) {
                        env.executor.execute(new LocalAppTask(
                                env.outputFilesPaths[Integer.parseInt(inputIndex)],
                                summaryFileName, // S3 bucket key
                                bucketName));
                        aws.s3.deleteFileFromS3(bucketName, summaryFileName);
                        filesLeftToProcess--;
                    }
                    aws.sqs.deleteMessage(managerToLocalQueueUrl, response);
                }

            }
        }

    }

    private static void waitForExecutorToFinish(ThreadPoolExecutor executor) {
        System.out.println("[DEBUG] Waiting for executor to finish");

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


