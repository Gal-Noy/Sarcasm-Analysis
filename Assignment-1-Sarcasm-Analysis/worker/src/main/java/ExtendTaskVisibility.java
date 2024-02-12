import software.amazon.awssdk.services.sqs.model.Message;

public class ExtendTaskVisibility implements Runnable{
    private final AWS aws = AWS.getInstance();
    private final Message task;
    private final String queueUrl;

    public ExtendTaskVisibility(Message task, String queueUrl) {
        this.task = task;
        this.queueUrl = queueUrl;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(AWSConfig.VISIBILITY_TIMEOUT * 1000 / 2);
                aws.sqs.changeMessageVisibility(queueUrl, task, 2 * AWSConfig.VISIBILITY_TIMEOUT);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
