import java.io.File;
import java.util.Scanner;

import org.json.*;

public class Env {
    public boolean terminate;
    public int reviewsPerWorker; // n
    public int numberOfFiles; // N
    public String[] inputFiles; // length = N
    public String[] outputFiles; // length = N
    public int numberOfWorkers; // m


    public Env(String[] args) {
        int numberOfArguments = args.length;

        terminate = numberOfArguments % 2 == 0;

        reviewsPerWorker = Integer.parseInt(args[numberOfArguments - (terminate ? 2 : 1)]);

        numberOfFiles = (numberOfArguments - 1) / 2;
        inputFiles = new String[numberOfFiles];
        outputFiles = new String[numberOfFiles];

        int numberOfReviews = 0;

        for (int i = 0; i < numberOfFiles; i++) {
            inputFiles[i] = args[i];
            outputFiles[i] = args[i + numberOfFiles];
            numberOfReviews += getNumberOfReviews(inputFiles[i]);
        }

        numberOfWorkers = (int) Math.ceil((double) numberOfReviews / reviewsPerWorker);
    }

    private int getNumberOfReviews(String inputFile) {
        int numberOfReviews = 0;
        try {
            Scanner scanner = new Scanner(new File(inputFile));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.contains("reviews")) {
                    JSONObject jsonObject = new JSONObject(line);
                    numberOfReviews += jsonObject.getJSONArray("reviews").length();
                }
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return numberOfReviews;
    }
}
