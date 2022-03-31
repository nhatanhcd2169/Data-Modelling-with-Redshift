import boto3
import json
import configparser
import time

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

# AWS & IAM
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
ROLE_NAME = config.get("IAM_ROLE", "ROLE_NAME")
# CLUSTER
RS_ID = config.get("DWH", "RS_ID")
RS_TYPE = config.get("DWH", "RS_TYPE")
RS_NODE_TYPE = config.get("DWH", "RS_NODE_TYPE")
RS_NUM_OF_NODES = config.get("DWH", "RS_NUM_OF_NODES")
# DB
DB_NAME = config.get("DB", "DB_NAME")
DB_USER = config.get("DB", "DB_USER")
DB_PASSWORD = config.get("DB", "DB_PASSWORD")
DB_PORT = config.get("DB", "DB_PORT")


def configure_vpc(ec2, redshift_props):
    try:
        vpc = ec2.Vpc(id=redshift_props["VpcId"])
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT),
        )
    except Exception as e:
        print(e)


def init():
    print("INITIALIZING . . . ")
    ec2 = boto3.resource(
        "ec2",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    s3 = boto3.resource(
        "s3",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    iam = boto3.client(
        "iam",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    dwhRole = iam.create_role(
        Path="/",
        RoleName=ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "redshift.amazonaws.com"},
                    }
                ],
                "Version": "2012-10-17",
            }
        ),
    )
    iam.attach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    ROLE_ARN = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    config.set("IAM_ROLE", "ROLE_ARN", ROLE_ARN)
    with open("dwh.cfg", "w") as cfg_write:
        config.write(cfg_write)
    print("CREATING REDSHIFT CLUSTER . . . ")
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=RS_TYPE,
            NodeType=RS_NODE_TYPE,
            NumberOfNodes=int(RS_NUM_OF_NODES),
            # Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=RS_ID,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[ROLE_ARN],
        )
    except Exception as e:
        print(e)

    time_start = time.time()
    redshift_props = redshift.describe_clusters(ClusterIdentifier=RS_ID)["Clusters"][0]
    while redshift_props["ClusterStatus"] != "available":
        redshift_props = redshift.describe_clusters(ClusterIdentifier=RS_ID)[
            "Clusters"
        ][0]
        time.sleep(15)
    time_end = time.time()
    print(f"Cluster is available (time elapsed: {int(time_end - time_start)}s)")
    DB_HOST = redshift_props["Endpoint"]["Address"]
    config.set("DB", "DB_HOST", DB_HOST)
    with open("dwh.cfg", "w") as cfg_write:
        config.write(cfg_write)
    print("CLUSTER SUCCESSFULLY CREATED!")
    # RUN THIS ONLY WHEN YOU NEED TO OPEN INCOMING TCP PORT
    # configure_vpc(ec2, redshift_props)


def cleanup():
    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    iam = boto3.client(
        "iam",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    print("DELETING REDSHIFT CLUSTER . . . ")
    redshift.delete_cluster(ClusterIdentifier=RS_ID, SkipFinalClusterSnapshot=True)
    time_start = time.time()
    redshift_props = redshift.describe_clusters(ClusterIdentifier=RS_ID)["Clusters"][0]
    while redshift_props["ClusterStatus"] != "available":
        redshift_props = redshift.describe_clusters(ClusterIdentifier=RS_ID)[
            "Clusters"
        ][0]
        time.sleep(15)
    time_end = time.time()
    print(f"Cluster is deleted (time elapsed: {int(time_end - time_start)}s)")
    iam.detach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam.delete_role(RoleName=ROLE_NAME)
