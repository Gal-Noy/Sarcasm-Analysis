import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;


public class AWS {
    public static final Region REGION = Region.US_EAST_1;
    private static final AWS instance = new AWS();

    public static AWS getInstance() {
        return instance;
    }

    // EC2
    private final Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
    public final String amiId = "";
    public final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;

    public final String MANAGER_INSTANCE_SCRIPT = "a";
    public final String WORKER_INSTANCE_SCRIPT = "a";

    public void runManager() {
        // Check if a manager node exists
        boolean managerActive = false;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals("Type") && tag.value().equals("Manager")) {
                        managerActive = true;
                        // Check if the manager is active
                        if (!instance.state().name().equals(InstanceStateName.RUNNING) &&
                                !instance.state().name().equals(InstanceStateName.PENDING)) {
                            // Start the manager
                            ec2.startInstances(StartInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                            break;
                        }
                    }
                }
            }
        }
        if (!managerActive) {
            createManagerInstance();
        }
    }

    private void createManagerInstance() {
        createEC2(MANAGER_INSTANCE_SCRIPT, "Manager", INSTANCE_TYPE);
    }

    public String createEC2(String script, String tagName, InstanceType instanceType) {
        Ec2Client ec2 = Ec2Client.builder().region(REGION).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(instanceType)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Type")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, amiId);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public void terminateEC2(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        ec2.terminateInstances(request);
    }


    // S3
    private final S3Client s3 = S3Client.builder().region(REGION).build();
    public final String BUCKET_PREFIX = "sarcasm-analysis-bucket";
    public String BUCKET_NAME;

    public void createS3Bucket(String localAppId) {
        try {
            BUCKET_NAME = BUCKET_PREFIX + "-" + localAppId;
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(BUCKET_NAME)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(BUCKET_NAME)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void uploadFileToS3(String inputFile) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(inputFile)
                .build();
        s3.putObject(objectRequest, RequestBody.fromFile(new File(inputFile)));
    }

    // SQS
    private final SqsClient sqs = SqsClient.builder().region(REGION).build();
    public final String MANAGER_REQUEST_QUEUE_PREFIX = "managerRequestQueue";
    public String MANAGER_REQUEST_QUEUE_NAME;
    public final String MANAGER_RESPONSE_QUEUE_PREFIX = "managerResponseQueue";
    public String MANAGER_RESPONSE_QUEUE_NAME;
    public final String WORKER_REQUEST_QUEUE_PREFIX = "workerRequestQueue";
    public String WORKER_REQUEST_QUEUE_NAME;
    public final String WORKER_RESPONSE_QUEUE_PREFIX = "workerResponseQueue";
    public String WORKER_RESPONSE_QUEUE_NAME;

    public void createQueues(String localAppId) {
        MANAGER_REQUEST_QUEUE_NAME = MANAGER_REQUEST_QUEUE_PREFIX + "-" + localAppId;
        MANAGER_RESPONSE_QUEUE_NAME = MANAGER_RESPONSE_QUEUE_PREFIX + "-" + localAppId;
        WORKER_REQUEST_QUEUE_NAME = WORKER_REQUEST_QUEUE_PREFIX + "-" + localAppId;
        WORKER_RESPONSE_QUEUE_NAME = WORKER_RESPONSE_QUEUE_PREFIX + "-" + localAppId;

        createQueue(MANAGER_REQUEST_QUEUE_NAME);
        createQueue(MANAGER_RESPONSE_QUEUE_NAME);
        createQueue(WORKER_REQUEST_QUEUE_NAME);
        createQueue(WORKER_RESPONSE_QUEUE_NAME);
    }

    public void createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(request);
    }

    public void deleteQueue(String queueName) {
        DeleteQueueRequest request = DeleteQueueRequest.builder()
                .queueUrl(queueName)
                .build();
        sqs.deleteQueue(request);
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(request).queueUrl();
    }

    public void sendMessage(String queueName, String message) {
        String queueUrl = getQueueUrl(queueName);
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build());
    }
    public void sendMessages(String queueName, List<String> messages) {
        for (String message : messages) {
            sendMessage(queueName, message);
        }
    }

    public List<Message> receiveMessages(String queueName) {
        String queueUrl = getQueueUrl(queueName);
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(20)
                .build();
        return sqs.receiveMessage(request).messages();
    }

    public void deleteMessages(String queueName, List<Message> messages) {
        for (Message message : messages) {
            deleteMessage(queueName, message);
        }
    }

    private void deleteMessage(String queueName, Message message) {
        String queueUrl = getQueueUrl(queueName);
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(request);
    }

    public void deleteAllMessages(String queueName) {
        String queueUrl = getQueueUrl(queueName);
        List<Message> messages = receiveMessages(queueName);
        for (Message message : messages) {
            deleteMessage(queueName, message);
        }
    }

    public void deleteAllQueues() {
        deleteQueue(MANAGER_REQUEST_QUEUE_NAME);
        deleteQueue(MANAGER_RESPONSE_QUEUE_NAME);
        deleteQueue(WORKER_REQUEST_QUEUE_NAME);
        deleteQueue(WORKER_RESPONSE_QUEUE_NAME);
    }


}