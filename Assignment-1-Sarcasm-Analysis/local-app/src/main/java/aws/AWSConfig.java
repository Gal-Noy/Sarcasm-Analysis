package aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;

public class AWSConfig {
    public static Region REGION1 = Region.US_WEST_2; // for S3 and SQS
    public static Region REGION2 = Region.US_EAST_1; // for EC2
    public static final String ANALYZE_TASK = "analyze";
    public static final String TERMINATE_TASK = "terminate";
    public static final String RESPONSE_STATUS_DONE = "done";
    public static final String RESPONSE_STATUS_ERROR = "error";

    // S3
//    public static final String BUCKET_NAME = "sarcasm-analysis-bucket";
    public static final String BUCKET_NAME = "sarcasm-analysis-bucket-test";
    public static final String JARS_BUCKET_NAME = "sarcasm-analysis-jars-bucket";

    // EC2
    public static final String AMI_ID = "ami-00e95a9222311e8ed";
    public static final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;
    public static final String KEY_NAME = "vockey";

    public static final String MANAGER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "wget https://" + JARS_BUCKET_NAME + ".s3.amazonaws.com/manager.jar\n" +
            "java -jar manager.jar\n" +
            "shutdown -h now";

    public static final String MANAGER_TAG_TYPE = "Type";
    public static final String MANAGER_TAG_VALUE = "Manager";

    // SQS
    public static final String LOCAL_TO_MANAGER_QUEUE_NAME = "localToManagerQueue";
    public static final String MANAGER_TO_LOCAL_QUEUE_NAME = "managerToLocalQueue";

}
