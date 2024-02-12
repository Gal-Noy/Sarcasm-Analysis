import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
public class ManagerEnv {
    private static final AWS aws = AWS.getInstance();
    private static final ManagerEnv instance = new ManagerEnv();
    public static ManagerEnv getInstance() {
        return instance;
    }
    public boolean isTerminated = false;
    public String terminatingLocalAppId = "";
    public static int workers = 0;
    public ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    public List<Future<?>> pendingQueuePollers = new ArrayList<>(); // To wait for all queue pollers to finish at termination
    public List<String> polledQueues = new ArrayList<>();
    private static final Logger logger = LogManager.getLogger(ManagerEnv.class);

    public void assignWorkers(int workersNeeded) {
        logger.info("Assigning workers");

        int activeWorkers = aws.ec2.countActiveWorkers();
        int remainingWorkersCapacity = 18 - activeWorkers;

        if (remainingWorkersCapacity > 0) {
            int workersToCreate = Math.min(workersNeeded, remainingWorkersCapacity);
            for (int i = 0; i < workersToCreate; i++) {
                aws.ec2.createWorkerInstance();
            }
            workers += workersToCreate;
        }

        logger.info("Current workers: " + workers);
    }

    public void releaseWorkers(int workersToRelease) {
        logger.info("Releasing workers");

        int activeWorkers = aws.ec2.countActiveWorkers();
        for (int i = 0; i < Math.min(workersToRelease, activeWorkers); i++) {
            aws.ec2.terminateWorkerInstance();
        }
        workers -= workersToRelease;

        logger.info("Current workers: " + workers);
    }

}
