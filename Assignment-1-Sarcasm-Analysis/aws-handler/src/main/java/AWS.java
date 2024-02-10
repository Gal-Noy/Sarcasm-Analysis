import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWS {
    private static final AWS instance = new AWS();
    public static AWS getInstance() {
        return instance;
    }
    final Logger logger = LoggerFactory.getLogger(AWS.class);

    public final EC2Handler ec2 = new EC2Handler(logger);
    public final S3Handler s3 = new S3Handler(logger);
    public final SQSHandler sqs = new SQSHandler(logger);
}