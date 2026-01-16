import os, math, sys
from utilities.utilities import *

if __name__ == "__main__":
    exp_path = "/Users/amlalejini/Desktop/scratch/cse881"
    treatments = ["limited_res__rep_1"]
    cluster_treats = ["spec_cluster__nc_9__mode_both",
                      "spec_cluster__nc_5__mode_phenotype",
                      "spec_cluster__nc_5__mode_genotype",
                      "db_cluster__ep_0p1__mp_5__mode_phenotype"]

    orgdata_content = ""
    clusterinfo_content = ""#"treatment,cluster_params,time,cluster_id,back_cluster_id,num_correct,num_wrong,total,order".split(",")
    orgdata_header = ""
    clusterinfo_header = ""
    for treatment in treatments:
        treatment_path = os.path.join(exp_path, "final_out", treatment)
        for ctreat in cluster_treats:
            ctreat_path = os.path.join(treatment_path, ctreat)
            # Load cluster info data
            content = ""
            with open(os.path.join(ctreat_path, "clusterinfo.csv"), "r") as fp:
                content = fp.read().strip("\n").split("\n")
            clusterinfo_header = content[0]
            content = content[1:]
            for line in content:
                clusterinfo_content += treatment + "," + ctreat + "," + line + "\n"

            # Load org info data
            content = ""
            with open(os.path.join(ctreat_path, "orgdata.csv"), "r") as fp:
                content = fp.read().strip("\n").split("\n")
            orgdata_header = content[0]
            content = content[1:]
            for line in content:
                orgdata_content += "%s,%s,%s\n" % (treatment, ctreat, line)
    with open(os.path.join(exp_path, "final_out", "flat_orgdata.csv"), "w") as fp:
        orgdata_header = "treatment,cluster_params," + orgdata_header
        fp.write(orgdata_header + "\n" + orgdata_content)
    with open(os.path.join(exp_path, "final_out", "flat_clusterinfo.csv"), "w") as fp:
        clusterinfo_header = "treatment,cluster_params," + clusterinfo_header
        fp.write(clusterinfo_header + "\n" + clusterinfo_content)