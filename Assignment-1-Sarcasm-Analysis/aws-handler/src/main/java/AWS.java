import org.apache.logging.log4j.Logger;
public class AWS {
    private Logger logger;
    public EC2Handler ec2;
    public S3Handler s3;
    public SQSHandler sqs;

    public AWS(Logger logger) {
        this.logger = logger;
        this.ec2 = new EC2Handler(logger);
        this.s3 = new S3Handler(logger);
        this.sqs = new SQSHandler(logger);
    }
}