import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

public class EC2Handler {
    private final Ec2Client ec2 = Ec2Client.builder().region(AWSConfig.REGION1).build();
    private final Logger logger = LogManager.getLogger(EC2Handler.class);

    public void createEC2Instance(String script, String typeTagValue, String nameTagValue, InstanceType instanceType) {
        Ec2Client ec2 = Ec2Client.builder().region(AWSConfig.REGION1).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(instanceType)
                .imageId(AWSConfig.AMI_ID)
                .maxCount(1)
                .minCount(1)
                .keyName(AWSConfig.KEY_NAME)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name(AWSConfig.IAM_PROFILE).build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        CreateTagsRequest tagRequest = createTags(typeTagValue, nameTagValue, instanceId);

        try {
            ec2.createTags(tagRequest);
            logger.info("Successfully started EC2 instance " + instanceId + " based on AMI " + AWSConfig.AMI_ID);

        } catch (Ec2Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    private CreateTagsRequest createTags(String typeTagValue, String nameTagValue, String instanceId) {
        Tag typeTag = Tag.builder()
                .key(AWSConfig.TYPE_TAG)
                .value(typeTagValue)
                .build();

        Tag nameTag = Tag.builder()
                .key(AWSConfig.NAME_TAG)
                .value(nameTagValue)
                .build();

        return CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(typeTag, nameTag)
                .build();
    }

    public void runManager() {
        // Check if a manager node exists
        boolean isManagerExists = false;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals(AWSConfig.TYPE_TAG) && tag.value().equals(AWSConfig.MANAGER_TYPE_TAG_VALUE)) {
                        if (!instance.state().name().equals(InstanceStateName.TERMINATED) &&
                                !instance.state().name().equals(InstanceStateName.SHUTTING_DOWN)) {
                            isManagerExists = true;
                            logger.info("Manager instance " + instance.instanceId() + " is " + instance.state().name());
                            if (instance.state().name().equals(InstanceStateName.RUNNING) ||
                                    instance.state().name().equals(InstanceStateName.PENDING)) {
                                // If manager is running or pending, do nothing
                            } else {
                                // Terminate and create a new manager instance
                                terminateEC2Instance(instance.instanceId());
                                createManagerInstance();
                            }
                            return;
                        } else {
                            // Instance is terminated or shutting down, continue checking other instances
                            break;
                        }
                    }
                }
            }
        }
        if (!isManagerExists) {
            logger.info("Manager instance does not exist");
            createManagerInstance();
        }
    }

    private void createManagerInstance() {
        logger.info("Creating manager instance");
        createEC2Instance(AWSConfig.MANAGER_INSTANCE_SCRIPT, AWSConfig.MANAGER_TYPE_TAG_VALUE,
                AWSConfig.MANAGER_NAME_TAG_VALUE, AWSConfig.INSTANCE_TYPE);
    }

    public void createWorkerInstance() {
        logger.info("Creating worker instance");
        createEC2Instance(AWSConfig.WORKER_INSTANCE_SCRIPT, AWSConfig.WORKER_TYPE_TAG_VALUE,
                AWSConfig.WORKER_NAME_TAG_VALUE, AWSConfig.INSTANCE_TYPE);
    }

    public int countActiveWorkers() {
        int activeWorkers = 0;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals(AWSConfig.TYPE_TAG) && tag.value().equals(AWSConfig.WORKER_TYPE_TAG_VALUE)) {
                        if (instance.state().name().equals(InstanceStateName.RUNNING) ||
                                instance.state().name().equals(InstanceStateName.PENDING)) {
                            activeWorkers++;
                        }
                    }
                }
            }
        }
        logger.info("Active workers: " + activeWorkers);
        return activeWorkers;
    }

    public void terminateEC2Instance(String instanceId) {
        logger.info("Terminating EC2 instance " + instanceId);
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        ec2.terminateInstances(request);
    }

    public void terminateAllWorkers() {
        logger.info("Terminating all workers");
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals(AWSConfig.TYPE_TAG) && tag.value().equals(AWSConfig.WORKER_TYPE_TAG_VALUE)) {
                        if (instance.state().name().equals(InstanceStateName.RUNNING) ||
                                instance.state().name().equals(InstanceStateName.PENDING)) {
                            terminateEC2Instance(instance.instanceId());
                        }
                    }
                }
            }
        }
    }

    public void terminateManager() {
        logger.info("Terminating manager");
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals(AWSConfig.TYPE_TAG) && tag.value().equals(AWSConfig.MANAGER_TYPE_TAG_VALUE)) {
                        if (instance.state().name().equals(InstanceStateName.RUNNING) ||
                                instance.state().name().equals(InstanceStateName.PENDING)) {
                            terminateEC2Instance(instance.instanceId());
                        }
                    }
                }
            }
        }
    }

    public void terminateWorkerInstance() {
        logger.info("Terminating worker instance");
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals(AWSConfig.TYPE_TAG) && tag.value().equals(AWSConfig.WORKER_TYPE_TAG_VALUE)) {
                        if (instance.state().name().equals(InstanceStateName.RUNNING) ||
                                instance.state().name().equals(InstanceStateName.PENDING)) {
                            terminateEC2Instance(instance.instanceId());
                            return;
                        }
                    }
                }
            }
        }
    }
}
