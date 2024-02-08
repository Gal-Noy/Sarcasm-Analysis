import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

public class EC2Handler {
    private final Ec2Client ec2 = Ec2Client.builder().region(AWSConfig.REGION).build();

    public void createEC2Instance(String script, String typeTagValue, String nameTagValue, InstanceType instanceType) {
        Ec2Client ec2 = Ec2Client.builder().region(AWSConfig.REGION).build();
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
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, AWSConfig.AMI_ID);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
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
                            if (!instance.state().name().equals(InstanceStateName.RUNNING) &&
                                    !instance.state().name().equals(InstanceStateName.PENDING)) {
                                // Start the manager
                                ec2.startInstances(StartInstancesRequest.builder().instanceIds(instance.instanceId()).build());
                                break;
                            }
                        } else break;
                    }
                }
            }
        }
        if (!isManagerExists) {
            createManagerInstance();
        }
    }

    private void createManagerInstance() {
        createEC2Instance(AWSConfig.MANAGER_INSTANCE_SCRIPT, AWSConfig.MANAGER_TYPE_TAG_VALUE,
                AWSConfig.MANAGER_NAME_TAG_VALUE, AWSConfig.INSTANCE_TYPE);
    }

    public void createWorkerInstance() {
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
        return activeWorkers;
    }

}
