#!/usr/bin/env python
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process


def obtain_amount(artifact):
    if "Amount (ng)" in artifact.udf:
        return artifact.udf["Amount (ng)"]
    elif "Volume (ul)" in artifact.udf and "Concentration" in artifact.udf and  artifact.udf.get("Conc. Units") == "ng/ul":
        return artifact.udf["Volume (ul)"] * artifact.udf["Concentration"]
    else:
        raise Exception("Cannot identify the amount of material for artifact {}, sample {}".format(artifact.id))

def update_output_values(inp, out, amount):
    for field in ["Conc. Units", "Concentration"]:
        out.udf[field] = inp.udf[field]

    out.udf["Amount Left (ng)"] = amount
    out.put()


def main(args):
    log = []
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    process = Process(lims, id=args.pid)
    for io in process.input_output_maps:
        if io[1]['output-generation-type'] != 'PerInput':
            continue
        try:
            starting_amount = obtain_amount(io[0]['uri'])
        except Exception as e:
            log.append(str(e))
            starting_amount = 0

        log.append("Starting amount of {} : {} ng".format(io[0]['uri'].samples[0].name, starting_amount))
        current_amount = starting_amount
        #preps
        preps = lims.get_processes(inputartifactlimsid=io[0]['uri'].id, type=["Setup Workset/Plate", "Amount confirmation QC"])
        for pro in preps:
            if pro.id == args.pid:
                continue # skip the current step
            for prepio in pro.input_output_maps:
                if prepio[1]['output-generation-type'] == 'PerInput' and prepio[0]['uri'].id == io[0]['uri'].id:
                    if "Amount taken (ng)" in prepio[1]['uri'].udf: #should always be true
                        prep_amount = prepio[1]['uri'].udf["Amount taken (ng)"]
                        log.append("Removing {} ng for prep {} for sample {}".format(prep_amount, pro.id, io[0]['uri'].samples[0].name))
                        current_amount = current_amount - prep_amount
                    else:
                        log.append("No Amount Taken found for prep {} of sample {}".format(pro.id, io[0]['uri'].samples[0].name))

        if current_amount < 0:
            log.append("Estimated amount for sample {} is {}, correcting to zero".format(io[0]['uri'].samples[0].name, current_amount))
            current_amount = 0

        if current_amount < io[1]['uri'].udf["Amount taken (ng)"]:
            io[1]['uri'].qc_flag = "FAILED"
        else:
            io[1]['uri'].qc_flag = "PASSED"
        update_output_values(io[0]['uri'], io[1]['uri'], current_amount)

        with open("amount_check_log.txt", "w") as f:
            f.write("\n".join(log))

        for out in process.all_outputs():
            if out.name == "QC Assignment Log File" :
                for f in out.files:
                    lims.request_session.delete(f.uri)
                lims.upload_new_file(out, "amount_check_log.txt") 








if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--pid',
                        help='Lims id for current Process', required=True)
    args = parser.parse_args()
    main(args)