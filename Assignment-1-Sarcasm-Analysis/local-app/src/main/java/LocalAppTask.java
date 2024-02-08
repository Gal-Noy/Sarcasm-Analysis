import java.io.BufferedReader;
import java.io.FileWriter;
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
        try {
            InputStream summaryFile = aws.s3.downloadFileFromS3(bucketName, summaryFileName);
            String summaryText = new BufferedReader(
                    new InputStreamReader(summaryFile)).lines().collect(Collectors.joining(""));
            String[] summaryContent = summaryText.split(AWSConfig.SUMMARY_DELIMITER);

            StringBuilder htmlContent = new StringBuilder();

            htmlContent.append("<html><head><title>Review Summary</title></head><body>");

            for (String content : summaryContent) {
                // <review_id>::<review_rating>::<review_link>::<sentiment>::<entities>
                String[] reviewContent = content.split("::");
                String reviewId = reviewContent[0], reviewRating = reviewContent[1],
                        reviewLink = reviewContent[2], sentiment = reviewContent[3],
                        entities = reviewContent[4];

                String colorCode = getColorCode(sentiment);
                boolean isSarcastic = isSarcasticReview(Integer.parseInt(reviewRating));

                htmlContent.append("<div style=\"color:").append(colorCode).append(";\">")
                        .append("<a href=\"").append(reviewLink).append("\">Review Link</a><br>")
                        .append("Entities: [").append(entities).append("]<br>")
                        .append("Sarcasm: ").append(isSarcastic ? "Yes" : "No")
                        .append("</div><br>");
            }
            htmlContent.append("</body></html>");

            FileWriter htmlFile = new FileWriter(outputFilePath + ".html");
            htmlFile.write(htmlContent.toString());
            htmlFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[DEBUG] Summary file created at " + outputFilePath + ".html");
    }

    private String getColorCode(String sentiment) {
        return switch (sentiment) {
            case "very negative" -> "darkred";
            case "negative" -> "red";
            case "neutral" -> "black";
            case "positive" -> "lightgreen";
            case "very positive" -> "darkgreen";
            default -> "black";
        };
    }

    private boolean isSarcasticReview(int rating) {
        return rating <= 2;
    }


}
