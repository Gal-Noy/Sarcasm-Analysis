package aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;

public class AWSConfig {
    public static final Region REGION = Region.US_EAST_1;

    // S3
//    public static final String BUCKET_NAME = "sarcasm-analysis-bucket";
    public static final String BUCKET_NAME = "sarcasm-analysis-bucket-test";
    public static final String JARS_BUCKET_NAME = "sarcasm-analysis-jars-bucket";


    // EC2
    public static final String AMI_ID = "ami-0c231ed88473e8d57";
    public static final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;

    public static String MANAGER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "wget https://" + JARS_BUCKET_NAME + ".s3.amazonaws.com/manager.jar\n" +
            "java -jar manager.jar\n" +
            "shutdown -h now\n";
    public static final String WORKER_INSTANCE_SCRIPT = "a";


    // SQS
    public static final String LOCAL_TO_MANAGER_QUEUE_NAME = "localToManagerQueue";
    public static final String MANAGER_TO_WORKER_QUEUE_NAME = "managerToWorkerQueue";

}
