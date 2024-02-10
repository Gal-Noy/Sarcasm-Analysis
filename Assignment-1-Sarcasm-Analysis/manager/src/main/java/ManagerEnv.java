import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
public class ManagerEnv {
    public boolean isTerminated;
    public String terminatingLocalApp;
    public ThreadPoolExecutor executor;
    public int workers;
    List<String> polledQueues;


    public ManagerEnv() {
        isTerminated = false;
        terminatingLocalApp = null;
        workers = 0;
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.MAX_VALUE);
        polledQueues = new ArrayList<>();
    }
}
