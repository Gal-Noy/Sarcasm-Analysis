package aws;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

import static aws.AWSConfig.*;

public class EC2Handler {
    private final Ec2Client ec2 = Ec2Client.builder().region(REGION2).build();

    public void runManager() {
        // Check if a manager node exists
        boolean isManagerExists = false;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals(MANAGER_TAG_TYPE) && tag.value().equals(MANAGER_TAG_VALUE)) {
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
        createEC2Instance(MANAGER_INSTANCE_SCRIPT, MANAGER_TAG_VALUE, INSTANCE_TYPE);
    }

    public void createEC2Instance(String script, String tagName, InstanceType instanceType) {
        Ec2Client ec2 = Ec2Client.builder().region(REGION2).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(instanceType)
                .imageId(AMI_ID)
                .maxCount(1)
                .minCount(1)
                .keyName(KEY_NAME)
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
                    instanceId, AMI_ID);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
    }
}
