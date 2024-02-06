public class Env {
    public boolean terminate;
    public int reviewsPerWorker; // n
    public int numberOfFiles; // N
    public String[] inputFiles; // length = N
    public String[] outputFiles; // length = N


    public Env(String[] args) {
        int numberOfArguments = args.length;

        terminate = numberOfArguments % 2 == 0;

        reviewsPerWorker = Integer.parseInt(args[numberOfArguments - (terminate ? 2 : 1)]);

        numberOfFiles = (numberOfArguments - 1) / 2;
        inputFiles = new String[numberOfFiles];
        outputFiles = new String[numberOfFiles];

        for (int i = 0; i < numberOfFiles; i++) {
            inputFiles[i] = args[i];
            outputFiles[i] = args[i + numberOfFiles];
        }
    }
}
