import aws.AWS;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class LocalAppTask implements Runnable {
    private final AWS aws = AWS.getInstance();
    private final String outputFilePath;
    private final String summaryFileName;
    private final String bucketName;

    public LocalAppTask(String outputFilePath, String summaryFileName, String bucketName) {
        this.outputFilePath = outputFilePath;
        this.summaryFileName = summaryFileName;
        this.bucketName = bucketName;
    }

    @Override
    public void run() {
        String summaryMessage;
        try {
            InputStream inputStream = aws.s3.downloadFileFromS3(bucketName, summaryFileName);
            summaryMessage = new BufferedReader(
                    new InputStreamReader(inputStream)).lines().collect(Collectors.joining(""));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Create HTML output file

    }


}
