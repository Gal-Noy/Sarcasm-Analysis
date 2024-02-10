import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.util.UUID;

public class AWSConfig {
    public static Region REGION = Region.US_EAST_1;
    public static final String ANALYZE_TASK = "analyze";
    public static final String TERMINATE_TASK = "terminate";
    public static final String RESPONSE_STATUS_DONE = "done";
    public static final String RESPONSE_STATUS_ERROR = "error";
    public static final String SENTIMENT_ANALYSIS_TASK = "sentimentAnalysis";
    public static final String ENTITY_RECOGNITION_TASK = "entityRecognition";
    public static final String SUMMARY_FILE_INDICATOR = "summary";
    public static final String DEFAULT_DELIMITER = "-";
    public static final String MESSAGE_DELIMITER = "::";
    public static final String SUMMARY_DELIMITER = "##";

    // S3
    public static final String BUCKET_NAME = "sarcasm-analysis-bucket";
    public static final String JARS_BUCKET_NAME = "sarcasm-analysis-jars-bucket";

    // EC2
    public static final String AMI_ID = "ami-00e95a9222311e8ed";
    public static final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;
    public static final String KEY_NAME = "vockey";
    public static final String IAM_PROFILE = "LabInstanceProfile";

    public static final String MANAGER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "wget https://" + JARS_BUCKET_NAME + ".s3.amazonaws.com/manager.jar\n" +
            "java -jar manager.jar\n" +
            "shutdown -h now";

    public static String WORKER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "wget https://" + JARS_BUCKET_NAME + ".s3.amazonaws.com/worker.jar\n" +
            "java -jar worker.jar\n" +
            "shutdown -h now";

    public static final String TYPE_TAG = "Type";
    public static final String MANAGER_TYPE_TAG_VALUE = "Manager";
    public static final String WORKER_TYPE_TAG_VALUE = "Worker";
    public static final String NAME_TAG = "Name";
    public static final String MANAGER_NAME_TAG_VALUE = "sarcasm-analysis-manager";
    public static final String WORKER_NAME_TAG_VALUE = "sarcasm-analysis-worker-"
            + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

    // SQS
    public static final String LOCAL_TO_MANAGER_QUEUE_NAME = "localToManagerQueue";
    public static final String MANAGER_TO_LOCAL_QUEUE_NAME = "managerToLocalQueue";
    public static final String MANAGER_TO_WORKER_QUEUE_NAME = "managerToWorkerQueue";
    public static final String WORKER_TO_MANAGER_QUEUE_NAME = "workerToManagerQueue";


}
