import boto3
import argparse
from botocore.exceptions import ClientError


def create_iam_group(group_name, policies):
    # Create an IAM client
    iam = boto3.client('iam')
    
    try:
        # Create the IAM group
        iam.create_group(GroupName=group_name)
        print(f"IAM group {group_name} created successfully...")
        
        # Attach policies to the group
        for policy_arn in policies:
            iam.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
            print(f"\nAttached policy {policy_arn} to group {group_name}.")
    
    except ClientError as e:
        print(f"An error occurred while creating IAM group: {e}")


def create_iam_user(username, group_name):
    # Create an IAM client
    iam = boto3.client('iam')
    
    try:
        # Create the IAM user
        iam.create_user(UserName=username)
        
        # Add the user to the group
        iam.add_user_to_group(GroupName=group_name, UserName=username)
        
        print(f"\nIAM user '{username}' created and added to group '{group_name}'.")
    
    except ClientError as e:
        print(f"An error occurred while creating IAM user: {e}")


def main():
    parser = argparse.ArgumentParser(description='Create AWS IAM users in a group with full permissions.')
    parser.add_argument('--count', type=int, default=1, help='Number of IAM users to create (default is 1)')
    parser.add_argument('--username', required=True, help='Username for the IAM users')
    parser.add_argument('--group', required=True, help='Name of the IAM group')
    
    args = parser.parse_args()
    
    if args.count < 1:
        print("Count must be greater than or equal to 1.")
        return
    
    # Specify the policies you want to attach to the group
    policies = [
        "arn:aws:iam::aws:policy/AmazonEC2FullAccess",
        "arn:aws:iam::aws:policy/AWSCloudFormationFullAccess",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonRDSFullAccess"
    ]
    
    create_iam_group(args.group, policies)
    
    for i in range(args.count):
        username = f"{args.username}_{i+1}"
        create_iam_user(username, args.group)

if __name__ == '__main__':
    main()