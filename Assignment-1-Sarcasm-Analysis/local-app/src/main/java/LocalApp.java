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

        aws.ec2.runManager();
        aws.s3.createS3Bucket(localAppId + "::" + BUCKET_NAME);
        aws.sqs.createQueue(localAppId + "::" + LOCAL_TO_MANAGER_QUEUE_NAME);
        aws.sqs.createQueue(localAppId + "::" + MANAGER_TO_LOCAL_QUEUE_NAME);

        for (int inputIndex = 0; inputIndex < env.inputFiles.length; inputIndex++) {
            File inputFile = new File(env.inputFiles[inputIndex]);
            aws.s3.uploadFileToS3(localAppId + "::" + BUCKET_NAME, inputFile);
            aws.sqs.sendMessage(localAppId + "::" + LOCAL_TO_MANAGER_QUEUE_NAME,
                    String.format("%s::%s::%s::%s::%s",
                            localAppId, "analyze", inputFile.getName(), inputIndex, env.reviewsPerWorker));
        }

        int filesLeftToProcess = env.numberOfFiles;
        while (filesLeftToProcess > 0) {
            List<Message> receivedMessages = aws.sqs.receiveMessages(localAppId + "::" + MANAGER_TO_LOCAL_QUEUE_NAME);
            for (Message receivedMessage : receivedMessages) {
                String[] parts = receivedMessage.body().split("::");
                String receivedLocalAppId = parts[0], summary = parts[1], inputIndex = parts[2], status = parts[3]; // failed or done
                if (receivedLocalAppId.equals(localAppId)) {
                    if (status.equals("done")) {
                        env.executor.execute(new LocalAppTask(aws, env.outputFiles[Integer.parseInt(inputIndex) + env.numberOfFiles], summary));
                        aws.s3.deleteFileFromS3(localAppId + "::" + BUCKET_NAME, summary);
                        filesLeftToProcess--;
                    }
                }
            }
        }


        if (env.terminate) {
            aws.sqs.sendMessage(localAppId + "::" + LOCAL_TO_MANAGER_QUEUE_NAME,
                    String.format("%s::%s", localAppId, "terminate"));
        }

        env.executor.shutdown();

        while (true) {
            try {
                if (env.executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        aws.sqs.deleteQueue(localAppId + "::" + MANAGER_TO_LOCAL_QUEUE_NAME);
        aws.sqs.deleteQueue(localAppId + "::" + LOCAL_TO_MANAGER_QUEUE_NAME);
        aws.s3.deleteS3Bucket(localAppId + "::" + BUCKET_NAME);
        System.out.println("LocalApp finished");
    }

}
