import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
public class ManagerEnv {
    public boolean isTerminated;
    public String terminatingLocalApp;
    public ThreadPoolExecutor executor;
    int workers;

    public ManagerEnv() {
        isTerminated = false;
        terminatingLocalApp = null;
        workers = 0;
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.MAX_VALUE);
    }
}
