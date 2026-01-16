import botocore
from scale.server.server import Server

class SecurityGroup(Server):
    def __init__(self, 
                    ec2_environment='default',
                    region='us-east-1',
                    name=None, 
                    description=None,
                    vpc_id=None,
                    rules=[]):

        self.ec2_environment = ec2_environment
        self.region = region
        self.name = name
        self.description = description
        self.vpc_id = vpc_id
        self.rules = rules

        super(SecurityGroup, self).__init__(ec2_environment=self.ec2_environment, region=self.region)

    def create(self):
        if self.name is None:
            self.log.error('You must set a SecurityGroup name!')
            exit(1)
        if self.description is None:
            self.log.error('You must set a SecurityGroup description!')
            exit(1)
            

        params = {
            'GroupName': self.name,
            'Description': self.description
        }

        ec2 = self.session.resource('ec2')

        try:
            response = ec2.create_security_group(**params)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
                self.log.error('Security Group [{sg}] already exists!'.format(sg=self.name))
            return

        if len(self.rules) > 0:
            for rule in self.rules:
                self.add(sg=response.id, ip=rule['IP'], from_port=rule['FromPort'],
                                to_port=rule['ToPort'], protocol=rule['Protocol'])

        return response.id

    def add(self, sg, ip, from_port, to_port, protocol):
        ec2 = self.session.resource('ec2')
        security_group = ec2.SecurityGroup(sg)

        params = {
            'IpProtocol': protocol,
            'FromPort': int(from_port),
            'ToPort': int(to_port),
            'IpRanges': [{'CidrIp': ip}]
        }

        security_group.authorize_ingress(GroupId=sg, IpPermissions=[params])

