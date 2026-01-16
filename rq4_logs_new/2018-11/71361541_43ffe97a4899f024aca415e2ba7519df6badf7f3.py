#!/usr/bin/env python
DESC="""EPP script to set qc-flaggs based on given conditions

Maya Brandi
Science for Life Laboratory, Stockholm, Sweden
""" 
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process, Artifact
import sys



class SetQC:
    def __init__(self, process, lims):
        self.lims = lims
        self.conditions = []
        self.process = process
        self.iom = self.process.input_output_maps
        self.artifacts = []
        self.qc_fail = 0
        self.qc_pass = 0

    def get_tresholds(self, conditions):
        for condition in conditions:
            udf, criteria, treshold = condition.split(',')
            self.conditions.append({'udf': udf, 'criteria': criteria, 'treshold': treshold})
            
    def get_artifacts(self):
        artifact_ids = [io[1]['limsid'] for io in self.iom if io[1]['output-generation-type'] == 'PerInput']
        self.artifacts = [Artifact(self.lims, id=id) for id in artifact_ids if id is not None]

    def set_qc(self):
        if not self.conditions:
            return
        for art in self.artifacts:
            qc_flag = 'PASSED'
            for condition in self.conditions:
                udf = condition.get('udf')
                if not art.udf.get(udf):
                    sys.exit('Artifacts dont have the udf: '+ udf)

                udf_value = art.udf.get(udf)
                criteria = condition.get('criteria')
                treshold = float(condition.get('treshold'))

                if criteria == '>=':
                    if udf_value >= treshold:
                        qc_flag = "FAILED"
                elif criteria == '<=':
                    if udf_value <= treshold:
                        qc_flag = "FAILED"
                elif criteria == '>':
                    if udf_value > treshold:
                        qc_flag = "FAILED"
                elif criteria == '<':
                    if udf_value < treshold:
                        qc_flag = "FAILED"
                elif criteria == '==':
                    if udf_value == treshold:
                        qc_flag = "FAILED"
                elif criteria == '!=':
                    if udf_value != treshold:
                        qc_flag = "FAILED"

            if qc_flag=='FAILED':
                self.qc_fail+=1
            else:
                self.qc_pass+=1

            art.qc_flag = qc_flag
            art.put()

def main(lims,args):
    process = Process(lims, id = args.pid)
    C2QC = SetQC(process, lims)
    C2QC.get_artifacts()
    C2QC.get_tresholds(args.conditions)
    C2QC.set_qc()
    
    abstract = ''
    if C2QC.qc_pass:
        abstract += str(C2QC.qc_pass) + ' samples passed QC. ' 
    if C2QC.qc_fail:
        abstract += str(C2QC.qc_fail) + ' samples failed QC. '
        
    if C2QC.qc_fail:
        sys.exit(abstract)
    else:
        print >> sys.stderr, abstract


if __name__ == "__main__":
    # Initialize parser with standard arguments and description
    parser = ArgumentParser(description=DESC)
    parser.add_argument('-p', dest = 'pid',
                        help='Lims id for current Process')
    parser.add_argument('-c', dest = 'conditions', nargs='+',
                        help=('Strings with three elements comma separated describing a FAILING conditions: "<art udf1>,<criteria1>,<treshold1>" "<art udf2>,<criteria2>,<treshold2>" "<art udf3>,<criteria3>,<treshold3>" etc. Accepted failing conditions are: <, <=, >, >=, ==, !='))
    
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    main(lims, args)