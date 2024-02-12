import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.stream.Collectors;

public class LocalAppTask implements Runnable {
    private final AWS aws = AWS.getInstance();
    private final String localAppId;
    private final String outputFilePath;
    private final String summaryFileName;
    private final Logger logger = LogManager.getLogger(LocalApp.class);


    public LocalAppTask(String localAppId, String outputFilePath, String summaryFileName) {
        this.localAppId = localAppId;
        this.outputFilePath = outputFilePath;
        this.summaryFileName = summaryFileName;
    }

    @Override
    public void run() {
        logger.info("LocalAppTask started");
        try {
            InputStream summaryFile = aws.s3.downloadFileFromS3(AWSConfig.BUCKET_NAME,
                    localAppId + AWSConfig.BUCKET_KEY_DELIMITER + summaryFileName);
            String summaryText = new BufferedReader(
                    new InputStreamReader(summaryFile)).lines().collect(Collectors.joining(""));
            String[] summaryContent = summaryText.split(AWSConfig.SUMMARY_DELIMITER);

            logger.info("Received summary file from S3");

            StringBuilder htmlContent = new StringBuilder();

            htmlContent.append("<html><head><title>Reviews Summary</title>")
                    .append("<style>table {font-family: arial, sans-serif;border-collapse: collapse;}")
                    .append("td, th {border: 1px solid #dddddd;text-align: left;padding: 8px;}")
                    .append("tr:nth-child(even) {background-color: #dddddd;}</style></head>")
                    .append("<body><table><tr><th>Review Link</th><th>Entities</th><th>Sarcasm</th></tr>");

            for (String content : summaryContent) {
                // <review_id>::<review_rating>::<review_link>::<sentiment>::<entities>
                String[] reviewContent = content.split(AWSConfig.MESSAGE_DELIMITER, -1);
                String reviewId = reviewContent[0], reviewRating = reviewContent[1],
                        reviewLink = reviewContent[2], sentiment = reviewContent[3],
                        entities = reviewContent[4];

                String colorCode = getColorCode(Integer.parseInt(sentiment));
                String formattedEntities = entities.isEmpty() ? "" : "[" + entities + "]";
                boolean isSarcastic = isSarcasticReview(Integer.parseInt(reviewRating));

                htmlContent.append("<tr>")
                        .append(String.format("<td><a href=\"%s\" style=\"color:%s\">Review Link</a><br></td>", reviewLink, colorCode))
                        .append(String.format("<td>%s</td>", formattedEntities))
                        .append(String.format("<td>%s</td>", isSarcastic ? "Yes" : "No"))
                        .append("</tr>");

                logger.info("Added review " + reviewId + " to summary");
            }

            htmlContent.append("</table></body></html>");

            FileWriter htmlFile = new FileWriter(outputFilePath + ".html");
            htmlFile.write(htmlContent.toString());
            htmlFile.close();

            logger.info("Created summary HTML file at " + outputFilePath + ".html");

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private String getColorCode(int sentiment) {
        switch (sentiment) {
            case 0:
                return "darkred";
            case 1:
                return "red";
            case 2:
                return "black";
            case 3:
                return "lightgreen";
            case 4:
                return "darkgreen";
            default:
                return "black";
        }
    }

    private boolean isSarcasticReview(int rating) {
        return rating <= 2;
    }


}
