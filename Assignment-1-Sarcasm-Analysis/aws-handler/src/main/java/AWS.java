public class AWS {
    private static final AWS instance = new AWS();
    public static AWS getInstance() {
        return instance;
    }
    public final EC2Handler ec2 = new EC2Handler();
    public final S3Handler s3 = new S3Handler();
    public final SQSHandler sqs = new SQSHandler();
}