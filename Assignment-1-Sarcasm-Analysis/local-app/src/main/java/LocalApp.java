import aws.AWS;
import aws.AWSConfig;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class LocalApp {
    public static void main(String[] args) {
        int numberOfArguments = args.length;

        if (numberOfArguments < 3) {
            System.out.println("Usage: LocalApp <input_file1> ... <input_fileN> <output_file1> ... <output_fileN> <n> [terminate]");
            return;
        }

        String localAppId = UUID.randomUUID().toString();
        System.out.println("LocalApp started with id " + localAppId);

        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        Env env = new Env(args);
        AWS aws = AWS.getInstance();

//        aws.ec2.runManager();
        aws.s3.createS3Bucket();
        aws.sqs.createQueues();

        for (String inputFile : env.inputFiles) {
            File file = new File(inputFile);
            aws.s3.uploadFileToS3(file);
            String messageToManager = String.format("local::manager::%s::%s::%s::%s", "input-file", localAppId, file.getName(), env.reviewsPerWorker);
            aws.sqs.sendMessage(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME, messageToManager);
        }

        // Checks an SQS queue for a message indicating the process is done and the response (the summary files) are available on S3

        // Create html file showing the summary of the reviews

        // In case of terminate, send a message to the manager node to terminate the workers

        // IMPORTANT: There might be more than one local application running at the same time.

        // IMPORTANT: The local application should be able to handle the case where the manager node is not available.


        aws.sqs.deleteQueues();
        aws.s3.deleteS3Bucket();
        System.out.println("LocalApp finished");
    }

}
