import aws.AWS;

import java.util.List;
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
            List<String> requests = aws.sqs.getRequests();
        }


        // Distribute the operations to be performed on the reviews to the workers using SQS queue/s.


    }
}
