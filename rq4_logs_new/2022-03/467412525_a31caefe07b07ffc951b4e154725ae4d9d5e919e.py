import yaml
from datetime import datetime, timedelta
now = datetime.now()
with open(r'C:\Users\MANASA\Desktop\KLA-\kla.yaml') as f:
    yaml_content = yaml.safe_load(f)

    def seqfunction(act, name, now):
        f1 = open("output.txt", "w")
        for key in act:
            object = act[key]
            printobj = "; " + name+"."+key+" "
            fWriteOp = str(now)+printobj+"Entry\n"
            f1.write(fWriteOp)
            fl = 1
            for j in object:
                if(j == "Inputs"):
                    fl = 0
            if(fl == 0):
                inputs = object['Inputs']
                funname = object['Function']
                time = inputs['ExecutionTime']
                fWriteOp = ""
                fWriteOp = str(now)+printobj+"Executing" + \
                    funname+"("+inputs['FunctionInput']+","+time+")\n"
                f1.write(fWriteOp)
                now = now + timedelta(seconds=int(time))
                now += timedelta(seconds=int(time))
            fWriteOp = ""
            fWriteOp = str(now)+printobj+"Exit\n"
            f1.write(fWriteOp)
    mainservice = yaml_content['M1A_Workflow']
    name = "M1A_Workflow"
    for key in mainservice:
        if mainservice[key] == 'Sequential':
            seqfunction(mainservice['Activities'], name, now)