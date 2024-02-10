import org.apache.logging.log4j.Logger;

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
    private final Logger logger;

    public LocalAppTask(String outputFilePath, String summaryFileName, String bucketName, Logger logger) {
        this.outputFilePath = outputFilePath;
        this.summaryFileName = summaryFileName;
        this.bucketName = bucketName;
        this.logger = logger;
    }

    @Override
    public void run() {
        logger.info("[INFO] LocalAppTask started");
        try {
            InputStream summaryFile = aws.s3.downloadFileFromS3(bucketName, summaryFileName);
            String summaryText = new BufferedReader(
                    new InputStreamReader(summaryFile)).lines().collect(Collectors.joining(""));
            String[] summaryContent = summaryText.split(AWSConfig.SUMMARY_DELIMITER);

            logger.info("[INFO] Summary file downloaded from S3");

            StringBuilder htmlContent = new StringBuilder();

            htmlContent.append("""
                    <html>
                    <head>
                    <title>Reviews Summary</title>
                    <style>
                    table {
                      font-family: arial, sans-serif;
                      border-collapse: collapse;
                    }
                    td, th {
                      border: 1px solid #dddddd;
                      text-align: left;
                      padding: 8px;
                    }
                    tr:nth-child(even) {
                      background-color: #dddddd;
                    }
                    </style>
                    </head>
                    <body>
                    <table>
                      <tr>
                        <th>Review Link</th>
                        <th>Entities</th>
                        <th>Sarcasm</th>
                      </tr>""");

            logger.info("[INFO] Creating HTML content for summary file");

            for (String content : summaryContent) {
                // <review_id>::<review_rating>::<review_link>::<sentiment>::<entities>
                String[] reviewContent = content.split(AWSConfig.MESSAGE_DELIMITER, -1);
                String reviewRating = reviewContent[1],
                        reviewLink = reviewContent[2], sentiment = reviewContent[3],
                        entities = reviewContent[4];

                String colorCode = getColorCode(Integer.parseInt(sentiment));
                String formattedEntities = entities.isEmpty() ? "" : "[" + entities + "]";
                boolean isSarcastic = isSarcasticReview(Integer.parseInt(reviewRating));

                htmlContent.append(String.format("""
                        <tr>
                          <td><a href="%s" style="color:%s">Review Link</a><br></td>
                          <td>%s</td>
                          <td>%s</td>
                        </tr>""", reviewLink,
                        colorCode,
                        formattedEntities,
                        isSarcastic ? "Yes" : "No"));

                logger.info("[INFO] Review added to HTML content");
            }

            htmlContent.append("</table></body></html>");

            FileWriter htmlFile = new FileWriter(outputFilePath + ".html");
            htmlFile.write(htmlContent.toString());
            htmlFile.close();

            logger.info("[INFO] Summary file created at " + outputFilePath + ".html");

        } catch (Exception e) {
            logger.error("[ERROR] " + e.getMessage());
        }
    }

    private String getColorCode(int sentiment) {
        return switch (sentiment) {
            case 0 -> "darkred";
            case 1 -> "red";
            case 2 -> "black";
            case 3 -> "lightgreen";
            case 4 -> "darkgreen";
            default -> "black";
        };
    }

    private boolean isSarcasticReview(int rating) {
        return rating <= 2;
    }


}
