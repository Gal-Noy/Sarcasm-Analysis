import aws.AWS;
import aws.AWSConfig;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.thirdparty.jackson.core.JsonParser;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Manager {
    public static void main(String[] args) {
        System.out.println("Manager started");
        boolean isRunning = true;
        int workers = 0, activeWorkers = 0;
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        AWS aws = AWS.getInstance();

        while (isRunning) {
            List<String> requests = aws.sqs.receiveMessages(AWSConfig.LOCAL_TO_MANAGER_QUEUE_NAME)
                    .stream().map(Message::body).toList();
            for (String request : requests) {
                // request format: local::manager::<requestType>::<localAppId>::<inputFile>::<reviewsPerWorker>
                String[] requestContent = request.split("::");
                String requestType = requestContent[2];
                if (requestType.equals("terminate")) {
                    // Handle terminate
                    isRunning = false;
                } else {
                    String localAppId = requestContent[3];
                    InputStream inputFile = aws.s3.downloadFileFromS3(requestContent[4]);
                    System.out.println(inputFile);
//                    int reviewsPerWorker = Integer.parseInt(requestContent[5]);
//                    int totalReviews = getReviewsCount(inputFile);
//                    int workersNeeded = (int) Math.ceil((double) totalReviews / reviewsPerWorker);
//                    workers += workersNeeded;
//                    for (int i = 0; i < workersNeeded; i++) {
////                        executor.execute(new Worker(localAppId, inputFile, reviewsPerWorker));
//                        activeWorkers++;
//                    }
                }
            }
        }


        // Distribute the operations to be performed on the reviews to the workers using SQS queue/s.


    }

    private static int getReviewsCount(String inputFile) {
        int totalReviews = 0;
        try {
            Scanner scanner = new Scanner(new File(inputFile));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.contains("reviews")) {
                    JSONObject jsonObject = new JSONObject(line);
                    totalReviews += jsonObject.getJSONArray("reviews").length();
                }
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalReviews;
    }
}

