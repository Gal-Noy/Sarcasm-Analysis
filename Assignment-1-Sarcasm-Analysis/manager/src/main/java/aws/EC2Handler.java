package aws;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

import static aws.AWSConfig.*;

public class EC2Handler {
    private final Ec2Client ec2 = Ec2Client.builder().region(REGION2).build();

    public void createWorkerInstance() {
        createEC2Instance(WORKER_INSTANCE_SCRIPT, WORKER_TAG_VALUE, INSTANCE_TYPE);
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

    public int countActiveWorkers() {
        int activeWorkers = 0;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.key().equals("Type") && tag.value().equals("Worker")) {
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
