package aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;

public class AWSConfig {
    public static final Region REGION = Region.US_EAST_1;

    // S3
    public static final String BUCKET_NAME = "sarcasm-analysis-bucket";
    public static final String JARS_BUCKET_NAME = "sarcasm-analysis-jars";

    // EC2
    public static final String AMI_ID = "";
    public static final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;

    public static String MANAGER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "wget " + JARS_BUCKET_NAME + "/Manager.jar\n" +
            "java -jar Manager.jar <workers-num>\n" +
            "shutdown -h now\n";
    public static final String WORKER_INSTANCE_SCRIPT = "a";


    // SQS
    public static final String LOCAL_TO_MANAGER_QUEUE_NAME = "localToManagerQueue";
    public static final String MANAGER_TO_WORKER_QUEUE_NAME = "managerToWorkerQueue";

}
