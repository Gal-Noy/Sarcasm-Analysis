import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.ILoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

public class ExtendTaskVisibility implements Runnable{
    private final AWS aws = AWS.getInstance();
    private final Message task;
    private final String reviewId;
    private final String queueUrl;
    private int currVisibilityTimeout = AWSConfig.VISIBILITY_TIMEOUT;
    private final Logger logger = LogManager.getLogger(ExtendTaskVisibility.class);

    public ExtendTaskVisibility(Message task, String queueUrl) {
        this.task = task;
        this.queueUrl = queueUrl;
        this.reviewId = task.body().split(AWSConfig.MESSAGE_DELIMITER)[2];
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(currVisibilityTimeout * 1000L / 2);
                currVisibilityTimeout += AWSConfig.VISIBILITY_TIMEOUT;
                aws.sqs.changeMessageVisibility(queueUrl, task, currVisibilityTimeout);
                logger.info("Extended visibility timeout for task " + reviewId + " to " + currVisibilityTimeout);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
