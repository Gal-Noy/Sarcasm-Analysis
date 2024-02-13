package aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;


public class AWSConfig {
    public static Region REGION1 = Region.US_EAST_1;
    public static Region REGION2 = Region.US_WEST_2;
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
    public static final String JAR_BUCKET_NAME = "sarcasm-analysis-jar-bucket";
    public static final String BUCKET_KEY_DELIMITER = "/";

    // EC2
    public static final String AMI_ID = "ami-00e95a9222311e8ed";
    public static final InstanceType INSTANCE_TYPE = InstanceType.M4_LARGE;
    public static final String KEY_NAME = "vockey";
    public static final String IAM_PROFILE = "LabInstanceProfile";

    public static final String MANAGER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "aws s3 cp s3://" + JAR_BUCKET_NAME + "/sarcasm-analysis.jar sarcasm-analysis.jar\n" +
            "java -jar sarcasm-analysis.jar manager";

    public static final String WORKER_INSTANCE_SCRIPT = "#!/bin/bash\n" +
            "aws s3 cp s3://" + JAR_BUCKET_NAME + "/sarcasm-analysis.jar sarcasm-analysis.jar\n" +
            "java -jar sarcasm-analysis.jar worker";

    public static final String TYPE_TAG = "Type";
    public static final String MANAGER_TYPE_TAG_VALUE = "manager";
    public static final String WORKER_TYPE_TAG_VALUE = "worker";
    public static final String NAME_TAG = "Name";
    public static final String MANAGER_NAME_TAG_VALUE = "sarcasm-analysis-manager";
    public static final String WORKER_NAME_TAG_VALUE = "sarcasm-analysis-worker";
    public static final int MAXIMUM_WORKER_INSTANCES = 8;

    // SQS
    public static final String LOCAL_TO_MANAGER_QUEUE_NAME = "localToManagerQueue";
    public static final String MANAGER_TO_LOCAL_QUEUE_NAME = "managerToLocalQueue";
    public static final String MANAGER_TO_WORKER_QUEUE_NAME = "managerToWorkerQueue";
    public static final String WORKER_TO_MANAGER_QUEUE_NAME = "workerToManagerQueue";
    public static final int LONG_POLLING_TIME = 20;
    public static final int VISIBILITY_TIMEOUT = 60;
    public static final int RETURN_TASK_TIME = 1;


}
