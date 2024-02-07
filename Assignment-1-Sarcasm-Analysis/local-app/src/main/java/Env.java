import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Env {
    public boolean terminate;
    public int reviewsPerWorker; // n
    public int numberOfFiles; // N
    public String[] inputFilesPaths; // length = N
    public String[] outputFilesPaths; // length = N
    public ThreadPoolExecutor executor;


    public Env(String[] args) {
        int numberOfArguments = args.length;

        terminate = numberOfArguments % 2 == 0;

        reviewsPerWorker = Integer.parseInt(args[numberOfArguments - (terminate ? 2 : 1)]);

        numberOfFiles = (numberOfArguments - 1) / 2;
        inputFilesPaths = new String[numberOfFiles];
        outputFilesPaths = new String[numberOfFiles];

        for (int i = 0; i < numberOfFiles; i++) {
            inputFilesPaths[i] = args[i];
            outputFilesPaths[i] = args[i + numberOfFiles];
        }

        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberOfFiles);
    }

}
