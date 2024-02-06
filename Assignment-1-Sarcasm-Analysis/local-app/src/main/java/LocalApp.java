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

        aws.runManager();
        aws.createS3Bucket(localAppId);
        aws.createQueues(localAppId);

        for (String inputFile : env.inputFiles) {
            aws.uploadFileToS3(inputFile);
            aws.sendMessage(aws.MANAGER_REQUEST_QUEUE_NAME, inputFile);
        }

        // Checks an SQS queue for a message indicating the process is done and the response (the summary files) are available on S3

        // Create html file showing the summary of the reviews

        // In case of terminate, send a message to the manager node to terminate the workers

        // IMPORTANT: There might be more than one local application running at the same time.

        // IMPORTANT: The local application should be able to handle the case where the manager node is not available.

        System.out.println("LocalApp finished");
    }

}
