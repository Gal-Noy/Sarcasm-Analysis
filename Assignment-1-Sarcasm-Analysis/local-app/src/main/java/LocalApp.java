import aws.AWS;
import aws.AWSConfig;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static aws.AWSConfig.*;

public class LocalApp {
    private static final AWS aws = AWS.getInstance();
    private static final String localAppId = UUID.randomUUID().toString();
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: LocalApp <input_file1> ... <input_fileN> <output_file1> ... <output_fileN> <n> [terminate]");
            return;
        }
        System.out.println("[DEBUG] LocalApp started with id " + localAppId);

        LocalAppEnv env = new LocalAppEnv(args);

        String bucketName = BUCKET_NAME + "::" + localAppId;
        String localToManagerQueueName = LOCAL_TO_MANAGER_QUEUE_NAME + "::" + localAppId;
        String managerToLocalQueueName = MANAGER_TO_LOCAL_QUEUE_NAME + "::" + localAppId;

        aws.ec2.runManager();
        aws.s3.createS3Bucket(bucketName);
        aws.sqs.createQueue(localToManagerQueueName);
        aws.sqs.createQueue(managerToLocalQueueName);

        sendTasksToManager(env, bucketName, localToManagerQueueName);

        receiveResponsesFromManager(env, bucketName, managerToLocalQueueName);

        if (env.terminate) {
            System.out.println("[DEBUG] Sending terminate message to manager");

            // <local_app_id>::terminate
            aws.sqs.sendMessage(localToManagerQueueName,
                    String.join("::", localAppId, TERMINATE_TASK));
        }

        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);

        aws.sqs.deleteQueue(managerToLocalQueueName);
        aws.sqs.deleteQueue(localToManagerQueueName);
        aws.s3.emptyS3Bucket(bucketName);
        aws.s3.deleteS3Bucket(bucketName);

        System.out.println("[DEBUG] LocalApp finished");
    }

    private static void sendTasksToManager(LocalAppEnv env, String bucketName, String localToManagerQueueName) {
        for (int inputIndex = 0; inputIndex < env.inputFilesPaths.length; inputIndex++) {
            File inputFile = new File(env.inputFilesPaths[inputIndex]);
            aws.s3.uploadFileToS3(bucketName, inputFile);
            // <local_app_id>::analyze::<input_file>::<input_index>::<reviews_per_worker>
            aws.sqs.sendMessage(localToManagerQueueName, String.join("::",
                    localAppId,
                    ANALYZE_TASK,
                    inputFile.getName(), // S3 bucket key
                    Integer.toString(inputIndex),
                    env.reviewsPerWorker + ""));

            System.out.println("[DEBUG] Sent task to manager: " + inputFile.getName());
        }
    }

    private static void receiveResponsesFromManager(LocalAppEnv env, String bucketName, String managerToLocalQueueName) {
        int filesLeftToProcess = env.numberOfFiles;
        while (filesLeftToProcess > 0) {
            List<String> responses = aws.sqs.receiveMessages(managerToLocalQueueName)
                    .stream().map(Message::body).toList();
            for (String response : responses) {
                // <local_app_id>::<response_status>::<summary_file_name>::<input_index>
                String[] responseContent = response.split("::");
                String receivedLocalAppId = responseContent[0],
                        status = responseContent[1], // done or error
                        summaryFileName = responseContent[2],
                        inputIndex = responseContent[3];
                if (receivedLocalAppId.equals(localAppId)) {

                    System.out.println("[DEBUG] Received response from manager: " + summaryFileName + " " + status);

                    if (status.equals(RESPONSE_STATUS_DONE)) {
                        int outputPathIndex = Integer.parseInt(inputIndex) + env.numberOfFiles;
                        env.executor.execute(new LocalAppTask(
                                env.outputFilesPaths[outputPathIndex],
                                summaryFileName, // S3 bucket key
                                bucketName));
                        aws.s3.deleteFileFromS3(bucketName, summaryFileName);
                        filesLeftToProcess--;
                    }
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


