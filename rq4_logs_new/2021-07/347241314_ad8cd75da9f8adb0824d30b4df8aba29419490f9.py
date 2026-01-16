import glob
import json

import jinja2
from jinja2 import Environment, select_autoescape


def main():
    vpcs = load("./*vpcs.json", "Vpcs")
    subnets = load("./*subnets.json", "Subnets")
    nacls = load("./*nacls.json", "NetworkAcls")

    vpc_id_to_name = {}
    vpc_cidr_to_name = {}
    vpc_name_to_subnet_groups = {}
    vpc_name_to_cidrs = {}
    for vpc in vpcs:
        vpc_id = vpc["VpcId"]
        for tag in vpc["Tags"]:
            if tag['Key'] == "Name":
                name = tag['Value']
                vpc_id_to_name[vpc["VpcId"]] = name
                vpc_name_to_subnet_groups[name] = set()
                cidrs = []
                for assoc in vpc["CidrBlockAssociationSet"]:
                    vpc_cidr_to_name[assoc["CidrBlock"]] = name
                    cidrs.append(assoc["CidrBlock"])
                vpc_name_to_cidrs[name] = cidrs

    subnet_cidr_to_name = {}
    subnet_name_to_cidr = {}
    subnet_cidr_to_group = {}
    subnet_id_to_group = {}
    for subnet in subnets:
        for tag in subnet["Tags"]:
            if tag['Key'] == "Name":
                name = tag['Value']
                subnet_group = name[:-2]
                subnet_cidr_to_name[subnet["CidrBlock"]] = name
                subnet_cidr_to_group[subnet["CidrBlock"]] = subnet_group
                subnet_id_to_group[subnet["SubnetId"]] = subnet_group
                subnet_name_to_cidr[name] = subnet["CidrBlock"]
                vpc_name = vpc_id_to_name[subnet["VpcId"]]
                vpc_name_to_subnet_groups[vpc_name].add(subnet_group)

    extra_refs = {
        "universe": ["0.0.0.0/0"],
        "aws_s3": ["3.5.164.0/22", "3.5.168.0/23", "52.95.128.0/21"],
        "onprem_ldap": ["54.192.220.70/32"],
    }

    extra_ref_cidr_to_name = {}
    for key, value in extra_refs.items():
        for item in value:
            extra_ref_cidr_to_name[item] = key

    cidr_to_name = {**subnet_cidr_to_group, **vpc_cidr_to_name, **extra_ref_cidr_to_name}

    connections = set()
    for nacl in nacls:
        if not nacl["IsDefault"]:
            subnet_id = nacl["Associations"][0]["SubnetId"]
            subnet_group = subnet_id_to_group[subnet_id]
            for entry in nacl["Entries"]:
                if entry["Egress"] and entry["RuleAction"] == "allow":
                    if entry["Protocol"] == "-1":
                        connections.add((subnet_group, cidr_to_name.get(entry["CidrBlock"], entry["CidrBlock"]), "ALL"))
                    else:
                        port = entry["PortRange"]["From"]
                        if port != 1024:
                            connections.add(
                                (subnet_group, cidr_to_name.get(entry["CidrBlock"], entry["CidrBlock"]), port))
                elif entry["RuleAction"] == "allow" and subnet_cidr_to_group.get(entry["CidrBlock"]) is None:
                    port = entry["PortRange"]["From"]
                    if port != 1024:
                        connections.add(
                            (cidr_to_name.get(entry["CidrBlock"], entry["CidrBlock"]), subnet_group, port))

    jinja_vpcs = []
    for key, value in vpc_name_to_subnet_groups.items():
        jinja_vpcs.append((key, value))

    jinja_extra_cidrs = []
    for key, value in extra_refs.items():
        jinja_extra_cidrs.append((key, '\\n'.join(value)))

    jinja_vpc_cidrs = []
    for key, value in vpc_name_to_cidrs.items():
        jinja_vpc_cidrs.append((key, '\n'.join(value)))

    jinja_subnet_group_cidrs = []
    revdict = {}
    for k, v in subnet_cidr_to_group.items():
        revdict.setdefault(v, []).append(k)
    for key, value in revdict.items():
        jinja_subnet_group_cidrs.append((key, '\n'.join(value)))

    env = Environment(
        loader=jinja2.FileSystemLoader(searchpath="./templates"),
        autoescape=select_autoescape()
    )
    template = env.get_template("plantuml.jinja2")
    print(template.render(jinja_vpcs=jinja_vpcs, jinja_extra_cidrs=jinja_extra_cidrs, jinja_vpc_cidrs=jinja_vpc_cidrs,
                          jinja_subnet_group_cidrs=jinja_subnet_group_cidrs, connections=connections))
    print(vpcs)


def load(glob_pattern, list_name):
    out = []
    for filename in glob.glob(glob_pattern):
        with open(filename, 'r') as f:
            data = json.load(f)
            for item in data[list_name]:
                out.append(item)
    return out


if __name__ == "__main__":
    main()