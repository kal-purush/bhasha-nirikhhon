const { FRANKFURT } = require('../common/config');
const SubnetClass = require('../ec2/subnet.class');

const { VPC_A, VPC_B } = require('./vpc.list').VPC_ITEMS;

const items = {
  SUBNET_A_PRIVATE: {
    Region: FRANKFURT,
    Name: 'subnet-A-private',
    CidrBlock: '10.5.10.0/24',
    VpcName: VPC_A.Name
  },
  SUBNET_A_PUBLIC: {
    Region: FRANKFURT,
    Name: 'subnet-A-public',
    CidrBlock: '10.5.20.0/24',
    VpcName: VPC_A.Name
  },
  SUBNET_B_PRIVATE: {
    Region: FRANKFURT,
    Name: 'subnet-B-private',
    CidrBlock: '10.15.10.0/24',
    VpcName: VPC_B.Name
  },
  SUBNET_B_PUBLIC: {
    Region: FRANKFURT,
    Name: 'subnet-B-public',
    CidrBlock: '10.15.20.0/24',
    VpcName: VPC_B.Name
  }
};

Object.keys(items).forEach((key) => (items[key] = new SubnetClass(items[key])));

module.exports = {
  SUBNET_ITEMS: items,
  SUBNET_LIST: Object.values(items)
};