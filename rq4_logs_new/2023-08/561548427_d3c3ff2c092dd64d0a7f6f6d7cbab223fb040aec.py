import yaml

#if the below are present then use the relevant package
argparse_tags = ['from argparse', 'import argparse', 'ArgumentParser']
click_tags = ['from click', 'import click']

#
allowed_types = ['str','int','float','path','boolean']
allowed_args = ['type','default','tooltip','min_value','max_value','increment','user_defined','options','from_data','input_type']
boolean_values = ['True','False','true','false',True,False]

#add error handling


def _parse_input_python(file_loc):
    file = open(file_loc, 'r')
    Lines = file.readlines()
    line_array = []

    count = 0
    argparse_flag = False
    click_flag = False
    
    # Strips the newline character
    for line in Lines:
        count += 1
        line_array.append(line.strip())
        if any(tag in line for tag in argparse_tags):
            argparse_flag = True
        if any(tag in line for tag in click_tags):
            click_flag = True
    file.close()
    
    if (argparse_flag) and (not click_flag):
        print(f"argparse detected in {file_loc}")
        return(line_array, "argparse")
    elif (click_flag) and (not argparse_flag):
        print(f"click detected in {file_loc}")
        return(line_array, "click")
    elif (not click_flag) and (not argparse_flag):
        #raise exception: provide default template in UI
        print(f"Neither argparse nor click detected in {file_loc}")
    elif click_flag and argparse_flag:
        #raise exception: provide default template in UI
        print("Cannot discern if argparse or click are used")
        
        
def _dict_from_args(filelines, library):
    if library == 'argparse':
        arg_command = '.add_argument'
    elif library == 'click':
        arg_command = '.option'
        
    parameter_dict = {}
    for line in filelines:
        if arg_command in line:
            arg_string = line.split(f"{arg_command}(")[1]
            arguments = arg_string.split(',')
            argname = arguments[0].split('--')[1].strip().strip("'")
            parameter_dict[argname]={}
            for argument in arguments[1:]:
                argument_split = argument.strip().split('=')
                arg_value = ''.join(argument_split[1:])
                if len(arg_value.strip("'"))==0:
                    continue
                if arg_value.count('(')<arg_value.count(')'):
                    arg_value = arg_value.rstrip(')')
                if arg_value[0] == "'" and arg_value[-1] == "'":
                    arg_value = arg_value[1:-1]
                if len(argument_split)>1:
                    parameter_dict[argname][argument_split[0]] = arg_value
    return(parameter_dict)


def _stages_from_scripts(files):
    stages = {}
    #n.b. these should be provided in order
    for file in files:
        file_name = file.split('/')[-1].split('.py')[0]
        stages[file_name] = {'file' : file}
    return(stages)


def _is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False
    

def _attempt_numeric(string, ntype):
    if ntype == 'int':
        try:
            return int(string)
        except ValueError:
            return string
    if ntype == 'float':
        try:
            return float(string)
        except ValueError:
            return string        
        

def _format_argparse_parameters(parameters):
    for key, subdict in parameters.items():
        #inferring / correcting type 
        if 'type' in subdict:
            if subdict['type'] not in allowed_types:
                subdict['type'] = 'str'
        elif 'is_flag' in subdict:
            subdict['type'] = 'boolean'
            if 'default' not in subdict:
                subdict['default'] = False
        elif 'default' in subdict:
            if subdict['default'] in boolean_values:
                subdict['type'] = 'boolean'
            elif subdict['default'].isdigit():
                subdict['type'] = 'int'
            elif _is_float(subdict['default']):
                subdict['type'] = 'float'
            else:
                subdict['type'] = 'str'
        #adjusting naming conventions
        if 'help' in subdict:
            parameters[key]['tooltip'] = parameters[key].pop('help')
        if 'choices' in subdict:
            parameters[key]['options'] = parameters[key].pop('choices')
        if 'min' in subdict:
            parameters[key]['min_value'] = parameters[key].pop('min')
        if 'max' in subdict:
            parameters[key]['max_value'] = parameters[key].pop('max')
        #adjusting number formatting
        if 'type' in subdict:
            if subdict['type'] in ['int', 'float']:
                if 'default' in subdict:
                    subdict['default'] = _attempt_numeric(subdict['default'], subdict['type'])
                if 'min_value' in subdict:
                    subdict['min_value'] = _attempt_numeric(subdict['min_value'], subdict['type'])
                if 'max_value' in subdict:
                    subdict['max_value'] = _attempt_numeric(subdict['max_value'], subdict['type'])
                if 'increment' in subdict:
                    subdict['increment'] = _attempt_numeric(subdict['increment'], subdict['type'])
        #remove excess quotes from tooltip
        if 'tooltip' in subdict:
            raw_tip = subdict['tooltip'].strip("'")
            subdict['tooltip'] = str(raw_tip)
        #removing unknown arguments
        del_list = []
        for argkey in subdict.keys():
            if argkey not in allowed_args:
                del_list.append(argkey)
        [parameters[key].pop(argkey) for argkey in del_list]
    return(parameters)


def parameters_yaml_from_args(files, outfileloc = None):
    if type(files) == str:
        files = [files]
    parameters = {}
    
    for file in files:
        filelines, library = _parse_input_python(file)
        new_parameters = _dict_from_args(filelines, library)
        parameters = {**parameters, **new_parameters}
        #except:
            #print(f"Arguments not parsed for {file}. A default template will be presented instead")
    if library == 'argparse':
        formatted_parameters = _format_argparse_parameters(parameters)
    elif library == 'click':
        formatted_parameters = _format_argparse_parameters(parameters)   #mostly same for arguments we are using
    
    #stages = _stages_from_scripts(files)
    out_dict = {'parameters' : formatted_parameters,
                #'stages' : stages,
               }
    #input settings not covered
    #nor output settings
    
    if outfileloc:
        with open(outfileloc, 'w') as outfile:
            yaml.dump(out_dict, outfile, default_flow_style=False)
    yaml_obj = yaml.dump(out_dict, default_flow_style=False)
    return(yaml_obj)