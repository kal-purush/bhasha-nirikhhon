import boto3
import time
import json

# Initialize AWS clients
ec2 = boto3.client('ec2', region_name='us-west-1')
autoscaling = boto3.client('autoscaling', region_name='us-west-1')
elbv2 = boto3.client('elbv2', region_name='us-west-1')
iam = boto3.client('iam', region_name='us-west-1')
ecr = boto3.client('ecr', region_name='us-west-1')

def create_vpc():
    """Create VPC and related resources"""
    # Create VPC
    vpc_response = ec2.create_vpc(
        CidrBlock='10.0.0.0/16',
        TagSpecifications=[
            {
                'ResourceType': 'vpc',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrcVPC'}]
            }
        ]
    )
    vpc_id = vpc_response['Vpc']['VpcId']
    print(f"VPC created: {vpc_id}")

    # Wait for VPC to be available
    ec2.get_waiter('vpc_available').wait(VpcIds=[vpc_id])

    # Enable DNS hostnames for the VPC
    ec2.modify_vpc_attribute(
        VpcId=vpc_id,
        EnableDnsHostnames={'Value': True}
    )

    # Create Internet Gateway
    igw_response = ec2.create_internet_gateway(
        TagSpecifications=[
            {
                'ResourceType': 'internet-gateway',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrcIGW'}]
            }
        ]
    )
    igw_id = igw_response['InternetGateway']['InternetGatewayId']
    print(f"Internet Gateway created: {igw_id}")

    # Attach IGW to VPC
    ec2.attach_internet_gateway(
        InternetGatewayId=igw_id,
        VpcId=vpc_id
    )
    print(f"Internet Gateway {igw_id} attached to VPC {vpc_id}")

    # Create public and private subnets in different AZs
    az_response = ec2.describe_availability_zones(
        Filters=[{'Name': 'region-name', 'Values': ['us-west-1']}]
    )
    
    available_azs = [az['ZoneName'] for az in az_response['AvailabilityZones']]
    
    # Ensure we have at least 2 AZs
    if len(available_azs) < 2:
        raise Exception("Need at least 2 availability zones")
    
    subnets = {
        'public': [],
        'private': []
    }
    
    # Create 2 public and 2 private subnets (or as many as AZs available)
    for i, az in enumerate(available_azs[:2]):
        # Public subnet
        public_subnet = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=f'10.0.{i*2}.0/24',
            AvailabilityZone=az,
            TagSpecifications=[
                {
                    'ResourceType': 'subnet',
                    'Tags': [{'Key': 'Name', 'Value': f'ProjectOrc-Public-{i+1}'}]
                }
            ]
        )
        public_subnet_id = public_subnet['Subnet']['SubnetId']
        subnets['public'].append(public_subnet_id)
        print(f"Public subnet created: {public_subnet_id} in {az}")
        
        # Enable auto-assign public IP for public subnets
        ec2.modify_subnet_attribute(
            SubnetId=public_subnet_id,
            MapPublicIpOnLaunch={'Value': True}
        )
        
        # Private subnet
        private_subnet = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock=f'10.0.{i*2+1}.0/24',
            AvailabilityZone=az,
            TagSpecifications=[
                {
                    'ResourceType': 'subnet',
                    'Tags': [{'Key': 'Name', 'Value': f'ProjectOrc-Private-{i+1}'}]
                }
            ]
        )
        private_subnet_id = private_subnet['Subnet']['SubnetId']
        subnets['private'].append(private_subnet_id)
        print(f"Private subnet created: {private_subnet_id} in {az}")
    
    # Create route tables
    # Public route table
    public_rt = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'route-table',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-PublicRT'}]
            }
        ]
    )
    public_rt_id = public_rt['RouteTable']['RouteTableId']
    print(f"Public route table created: {public_rt_id}")
    
    # Add route to IGW
    ec2.create_route(
        RouteTableId=public_rt_id,
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=igw_id
    )
    
    # Associate public subnets with public route table
    for subnet_id in subnets['public']:
        ec2.associate_route_table(
            RouteTableId=public_rt_id,
            SubnetId=subnet_id
        )
        print(f"Associated public subnet {subnet_id} with route table {public_rt_id}")
    
    # Private route table
    private_rt = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'route-table',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-PrivateRT'}]
            }
        ]
    )
    private_rt_id = private_rt['RouteTable']['RouteTableId']
    print(f"Private route table created: {private_rt_id}")
    
    # Associate private subnets with private route table
    for subnet_id in subnets['private']:
        ec2.associate_route_table(
            RouteTableId=private_rt_id,
            SubnetId=subnet_id
        )
        print(f"Associated private subnet {subnet_id} with route table {private_rt_id}")
    
    # Create NAT Gateway for outbound traffic from private subnets
    eip = ec2.allocate_address(Domain='vpc')
    nat_gateway = ec2.create_nat_gateway(
        AllocationId=eip['AllocationId'],
        SubnetId=subnets['public'][0],
        TagSpecifications=[
            {
                'ResourceType': 'natgateway',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-NAT'}]
            }
        ]
    )
    nat_gateway_id = nat_gateway['NatGateway']['NatGatewayId']
    print(f"NAT Gateway created: {nat_gateway_id}")
    
    # Wait for NAT Gateway to be available
    print("Waiting for NAT Gateway to be available...")
    waiter = ec2.get_waiter('nat_gateway_available')
    waiter.wait(NatGatewayIds=[nat_gateway_id])
    
    # Add route to NAT Gateway for private subnets
    ec2.create_route(
        RouteTableId=private_rt_id,
        DestinationCidrBlock='0.0.0.0/0',
        NatGatewayId=nat_gateway_id
    )
    print(f"Added route to NAT Gateway for private route table")
    
    return {
        'vpc_id': vpc_id,
        'igw_id': igw_id, 
        'subnets': subnets,
        'public_rt_id': public_rt_id,
        'private_rt_id': private_rt_id,
        'nat_gateway_id': nat_gateway_id
    }

def create_security_groups(vpc_id):
    """Create security groups for load balancer and backend services"""
    # ALB Security Group
    alb_sg = ec2.create_security_group(
        GroupName='ProjectOrc-ALB-SG',
        Description='Security group for application load balancer',
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'security-group',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-ALB-SG'}]
            }
        ]
    )
    alb_sg_id = alb_sg['GroupId']
    print(f"ALB Security Group created: {alb_sg_id}")
    
    # Allow HTTP and HTTPS from anywhere to ALB
    ec2.authorize_security_group_ingress(
        GroupId=alb_sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 80,
                'ToPort': 80,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 443,
                'ToPort': 443,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }
        ]
    )
    
    # Backend Services Security Group
    backend_sg = ec2.create_security_group(
        GroupName='ProjectOrc-Backend-SG',
        Description='Security group for backend services',
        VpcId=vpc_id,
        TagSpecifications=[
            {
                'ResourceType': 'security-group',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-Backend-SG'}]
            }
        ]
    )
    backend_sg_id = backend_sg['GroupId']
    print(f"Backend Security Group created: {backend_sg_id}")
    
    # Allow traffic from ALB to backend services (port 3001 and 3002)
    ec2.authorize_security_group_ingress(
        GroupId=backend_sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 3001,
                'ToPort': 3001,
                'UserIdGroupPairs': [{'GroupId': alb_sg_id}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 3002,
                'ToPort': 3002,
                'UserIdGroupPairs': [{'GroupId': alb_sg_id}]
            }
        ]
    )
    
    # Allow SSH access for development/debugging
    ec2.authorize_security_group_ingress(
        GroupId=backend_sg_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'SSH access'}]
            }
        ]
    )
    
    return {
        'alb_sg_id': alb_sg_id,
        'backend_sg_id': backend_sg_id
    }

def create_load_balancer(subnets, alb_sg_id):
    """Create Application Load Balancer"""
    load_balancer = elbv2.create_load_balancer(
        Name='ProjectOrc-ALB',
        Subnets=subnets['public'],
        SecurityGroups=[alb_sg_id],
        Scheme='internet-facing',
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-ALB'}],
        Type='application',
        IpAddressType='ipv4'
    )
    
    alb_arn = load_balancer['LoadBalancers'][0]['LoadBalancerArn']
    alb_dns = load_balancer['LoadBalancers'][0]['DNSName']
    print(f"Application Load Balancer created: {alb_arn}")
    print(f"ALB DNS Name: {alb_dns}")
    
    # Create target groups for helloService and profileService
    hello_target_group = elbv2.create_target_group(
        Name='ProjectOrc-Hello-TG',
        Protocol='HTTP',
        Port=3001,
        VpcId=vpc_id,
        HealthCheckProtocol='HTTP',
        HealthCheckPath='/health',
        HealthCheckEnabled=True,
        HealthCheckIntervalSeconds=30,
        HealthCheckTimeoutSeconds=5,
        HealthyThresholdCount=2,
        UnhealthyThresholdCount=2,
        TargetType='instance',
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-Hello-TG'}]
    )
    
    hello_tg_arn = hello_target_group['TargetGroups'][0]['TargetGroupArn']
    print(f"Hello Service Target Group created: {hello_tg_arn}")
    
    profile_target_group = elbv2.create_target_group(
        Name='ProjectOrc-Profile-TG',
        Protocol='HTTP',
        Port=3002,
        VpcId=vpc_id,
        HealthCheckProtocol='HTTP',
        HealthCheckPath='/health',
        HealthCheckEnabled=True,
        HealthCheckIntervalSeconds=30,
        HealthCheckTimeoutSeconds=5,
        HealthyThresholdCount=2,
        UnhealthyThresholdCount=2,
        TargetType='instance',
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-Profile-TG'}]
    )
    
    profile_tg_arn = profile_target_group['TargetGroups'][0]['TargetGroupArn']
    print(f"Profile Service Target Group created: {profile_tg_arn}")
    
    # Create listeners to forward traffic to the target groups
    hello_listener = elbv2.create_listener(
        LoadBalancerArn=alb_arn,
        Protocol='HTTP',
        Port=80,
        DefaultActions=[
            {
                'Type': 'forward',
                'TargetGroupArn': hello_tg_arn
            }
        ],
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-Hello-Listener'}]
    )
    
    print(f"Hello Service Listener created")
    
    # Create a rule for the profile service using a path pattern
    profile_listener_rule = elbv2.create_rule(
        ListenerArn=hello_listener['Listeners'][0]['ListenerArn'],
        Conditions=[
            {
                'Field': 'path-pattern',
                'Values': ['/profile*']
            }
        ],
        Priority=10,
        Actions=[
            {
                'Type': 'forward',
                'TargetGroupArn': profile_tg_arn
            }
        ],
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-Profile-Rule'}]
    )
    
    print(f"Profile Service Listener Rule created")
    
    return {
        'alb_arn': alb_arn,
        'alb_dns': alb_dns,
        'hello_tg_arn': hello_tg_arn,
        'profile_tg_arn': profile_tg_arn
    }

def create_iam_role():
    """Create IAM Role for EC2 instances to access ECR"""
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    role = iam.create_role(
        RoleName='ProjectOrc-EC2-ECR-Role',
        AssumeRolePolicyDocument=json.dumps(assume_role_policy),
        Description='Role for EC2 instances to pull from ECR',
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-EC2-ECR-Role'}]
    )
    
    role_name = role['Role']['RoleName']
    role_arn = role['Role']['Arn']
    print(f"IAM Role created: {role_name} - {role_arn}")
    
    # Attach policies for ECR access
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonECR-FullAccess'
    )
    
    print(f"Attached ECR policy to role {role_name}")
    
    # Create instance profile and add role to it
    instance_profile = iam.create_instance_profile(
        InstanceProfileName='ProjectOrc-EC2-ECR-Profile',
        Tags=[{'Key': 'Name', 'Value': 'ProjectOrc-EC2-ECR-Profile'}]
    )
    
    iam.add_role_to_instance_profile(
        InstanceProfileName='ProjectOrc-EC2-ECR-Profile',
        RoleName=role_name
    )
    
    instance_profile_arn = instance_profile['InstanceProfile']['Arn']
    print(f"Instance Profile created: {instance_profile_arn}")
    
    # Wait for the instance profile to be ready
    print("Waiting for the instance profile to be ready...")
    time.sleep(15)
    
    return {
        'role_name': role_name,
        'role_arn': role_arn,
        'instance_profile_name': 'ProjectOrc-EC2-ECR-Profile',
        'instance_profile_arn': instance_profile_arn
    }

def create_launch_template(backend_sg_id, instance_profile_name):
    """Create Launch Template for Auto Scaling Group"""
    # Create user data script to pull and run docker images
    user_data = """#!/bin/bash
yum update -y
yum install -y docker
systemctl start docker
systemctl enable docker
amazon-linux-extras install -y aws-cli
aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 975050024946.dkr.ecr.us-west-1.amazonaws.com
docker pull 975050024946.dkr.ecr.us-west-1.amazonaws.com/aakash/project-orc-b:hello-latest
docker pull 975050024946.dkr.ecr.us-west-1.amazonaws.com/aakash/project-orc-b:profile-latest
docker run -d -p 3001:3001 975050024946.dkr.ecr.us-west-1.amazonaws.com/aakash/project-orc-b:hello-latest
docker run -d -p 3002:3002 975050024946.dkr.ecr.us-west-1.amazonaws.com/aakash/project-orc-b:profile-latest
"""
    
    import base64
    encoded_user_data = base64.b64encode(user_data.encode()).decode()
    
    # Create launch template
    launch_template = ec2.create_launch_template(
        LaunchTemplateName='ProjectOrc-BackendLT',
        VersionDescription='Initial version',
        TagSpecifications=[
            {
                'ResourceType': 'launch-template',
                'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-BackendLT'}]
            }
        ],
        LaunchTemplateData={
            'ImageId': 'ami-0c110e13b02dea71a',  # Amazon Linux 2 in us-west-1, update as needed
            'InstanceType': 't2.micro',
            'SecurityGroupIds': [backend_sg_id],
            'IamInstanceProfile': {
                'Name': instance_profile_name
            },
            'UserData': encoded_user_data,
            'TagSpecifications': [
                {
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': 'ProjectOrc-Backend'}]
                }
            ]
        }
    )
    
    lt_id = launch_template['LaunchTemplate']['LaunchTemplateId']
    lt_version = launch_template['LaunchTemplate']['LatestVersionNumber']
    print(f"Launch Template created: {lt_id}, version: {lt_version}")
    
    return {
        'lt_id': lt_id,
        'lt_version': lt_version
    }

def create_auto_scaling_group(lt_id, subnets, target_group_arns):
    """Create Auto Scaling Group for backend services"""
    asg = autoscaling.create_auto_scaling_group(
        AutoScalingGroupName='ProjectOrc-Backend-ASG',
        LaunchTemplate={
            'LaunchTemplateId': lt_id,
            'Version': '$Latest'
        },
        MinSize=2,
        MaxSize=5,
        DesiredCapacity=2,
        VPCZoneIdentifier=','.join(subnets['private']),
        TargetGroupARNs=target_group_arns,
        HealthCheckType='ELB',
        HealthCheckGracePeriod=300,
        Tags=[
            {
                'Key': 'Name',
                'Value': 'ProjectOrc-Backend-ASG',
                'PropagateAtLaunch': True
            }
        ]
    )
    
    print(f"Auto Scaling Group created: ProjectOrc-Backend-ASG")
    
    # Create scaling policies
    scale_up_policy = autoscaling.put_scaling_policy(
        AutoScalingGroupName='ProjectOrc-Backend-ASG',
        PolicyName='ProjectOrc-ScaleUp',
        PolicyType='TargetTrackingScaling',
        TargetTrackingConfiguration={
            'PredefinedMetricSpecification': {
                'PredefinedMetricType': 'ASGAverageCPUUtilization'
            },
            'TargetValue': 70.0
        }
    )
    
    print(f"Scale up policy created")
    
    return {
        'asg_name': 'ProjectOrc-Backend-ASG'
    }

def save_infrastructure_details(infrastructure):
    """Save infrastructure details to a file"""
    with open('infrastructure_details.json', 'w') as f:
        json.dump(infrastructure, f, indent=2)
    print("Infrastructure details saved to infrastructure_details.json")

if __name__ == "__main__":
    print("Starting infrastructure deployment...")
    
    # Create VPC and related resources
    vpc_info = create_vpc()
    vpc_id = vpc_info['vpc_id']
    
    # Create security groups
    sg_info = create_security_groups(vpc_id)
    
    # Create IAM role for EC2 to access ECR
    iam_info = create_iam_role()
    
    # Create launch template
    lt_info = create_launch_template(sg_info['backend_sg_id'], iam_info['instance_profile_name'])
    
    # Create load balancer and target groups
    lb_info = create_load_balancer(vpc_info['subnets'], sg_info['alb_sg_id'])
    
    # Create auto scaling group
    asg_info = create_auto_scaling_group(
        lt_info['lt_id'], 
        vpc_info['subnets'], 
        [lb_info['hello_tg_arn'], lb_info['profile_tg_arn']]
    )
    
    # Combine all information
    infrastructure = {
        'vpc': vpc_info,
        'security_groups': sg_info,
        'iam': iam_info,
        'launch_template': lt_info,
        'load_balancer': lb_info,
        'auto_scaling_group': asg_info
    }
    
    # Save infrastructure details
    save_infrastructure_details(infrastructure)
    
    print("Infrastructure deployment completed successfully!")