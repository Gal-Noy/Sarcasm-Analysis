import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
public class ManagerEnv {
    public boolean isTerminated;
    public ThreadPoolExecutor executor;
    public List<Future<?>> pendingQueuePollers; // To wait for all queue pollers to finish at termination
    public int workers;
    public List<String> polledQueues;


    public ManagerEnv() {
        isTerminated = false;
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.MAX_VALUE);
        pendingQueuePollers = new ArrayList<>();
        workers = 0;
        polledQueues = new ArrayList<>();
    }
}
