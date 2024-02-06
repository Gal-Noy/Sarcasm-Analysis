package aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

public class EC2Handler {
    private final Ec2Client ec2 = Ec2Client.builder().region(AWSConfig.REGION).build();

    public void runManager(int numberOfWorkers) {
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
            createManagerInstance(numberOfWorkers);
        }
    }

    private void createManagerInstance(int numberOfWorkers) {
        createEC2Instance(AWSConfig.MANAGER_INSTANCE_SCRIPT.replace("<workers-num>",
                String.valueOf(numberOfWorkers)), "Manager", AWSConfig.INSTANCE_TYPE);
    }

    public String createEC2Instance(String script, String tagName, InstanceType instanceType) {
        Ec2Client ec2 = Ec2Client.builder().region(AWSConfig.REGION).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(instanceType)
                .imageId(AWSConfig.AMI_ID)
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
                    instanceId, AWSConfig.AMI_ID);

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



}
