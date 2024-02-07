import aws.AWS;
import aws.AWSConfig;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static aws.AWSConfig.*;

public class LocalApp {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: LocalApp <input_file1> ... <input_fileN> <output_file1> ... <output_fileN> <n> [terminate]");
            return;
        }

        String localAppId = UUID.randomUUID().toString();
        System.out.println("LocalApp started with id " + localAppId);

        Env env = new Env(args);
        AWS aws = AWS.getInstance();

        String bucketName = BUCKET_NAME + "::" + localAppId;
        String localToManagerQueueName = LOCAL_TO_MANAGER_QUEUE_NAME + "::" + localAppId;
        String managerToLocalQueueName = MANAGER_TO_LOCAL_QUEUE_NAME + "::" + localAppId;

        aws.ec2.runManager();
        aws.s3.createS3Bucket(bucketName);
        aws.sqs.createQueue(localToManagerQueueName);
        aws.sqs.createQueue(managerToLocalQueueName);

        for (int inputIndex = 0; inputIndex < env.inputFilesPaths.length; inputIndex++) {
            File inputFile = new File(env.inputFilesPaths[inputIndex]);
            aws.s3.uploadFileToS3(bucketName, inputFile);
            aws.sqs.sendMessage(localToManagerQueueName, String.join("::",
                    localAppId,
                    "analyze",
                    inputFile.getName(), // S3 bucket key
                    Integer.toString(inputIndex),
                    env.reviewsPerWorker + ""));
        }

        int filesLeftToProcess = env.numberOfFiles;
        while (filesLeftToProcess > 0) {
            List<Message> receivedMessages = aws.sqs.receiveMessages(managerToLocalQueueName);
            for (Message receivedMessage : receivedMessages) {
                String[] parts = receivedMessage.body().split("::");
                String receivedLocalAppId = parts[0], summaryFileName = parts[1], inputIndex = parts[2], status = parts[3]; // failed or done
                if (receivedLocalAppId.equals(localAppId)) {
                    if (status.equals("done")) {
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


        if (env.terminate) {
            aws.sqs.sendMessage(localToManagerQueueName,
                    String.join("::", localAppId, "terminate"));
        }

        env.executor.shutdown();
        waitForExecutorToFinish(env.executor);

        aws.sqs.deleteQueue(managerToLocalQueueName);
        aws.sqs.deleteQueue(localToManagerQueueName);
        aws.s3.deleteS3Bucket(bucketName);

        System.out.println("LocalApp finished");
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


