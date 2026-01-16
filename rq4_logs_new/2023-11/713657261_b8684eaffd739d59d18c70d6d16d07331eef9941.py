"""An AWS Python Pulumi program"""


# from eb import eb_ecs


# conn = eb_ecs.create_rds()

# eb_ecs.create_elastic_beanstalk_with_ecs(conn)



from ec2.ec2 import launch_template, create_auto_scaling_group
from vpc.vpc import create_custom_vpc_and_subnets



(custom_vpc, subnet_a, subnet_b, igw, route_table,
 rt_assoc_a, rt_assoc_b, custom_sg) = create_custom_vpc_and_subnets()

subnet_ids = [subnet_a.id, subnet_a.id]
custom_sg_id= custom_sg.id


lt = launch_template(custom_sg_id)

create_auto_scaling_group(lt, subnet_ids
                          )