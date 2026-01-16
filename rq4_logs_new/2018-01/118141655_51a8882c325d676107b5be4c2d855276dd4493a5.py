#!/usr/bin/env python3
#
# Google Python Style Guide
# 
# module_name, package_name
# GLOBAL_CONSTANT_NAME, 
# global_var_name, local_var_name
# function_name, function_parameter_name
# ClassName, method_name, instance_var_name
# ExceptionName
#
from Globals import *
import os

class Builder():

    conf = OrderedDict()
    envvars = OrderedDict()
    docker_envvars = []

    def resolve_env_var(self, envvars):
        result = OrderedDict()
        for key,value in envvars.items():
            variable_name = str(key).upper()

            if type(value) not in [dict,OrderedDict]:
                result[variable_name] = value
            elif is_parsable(value):
                result[variable_name] = value
            else:
                prefix = variable_name + "_"
                resolved_vars = self.resolve_env_var(value)
                for key,value in resolved_vars.items():
                    variable_name = str(key).upper()
                    result[prefix + variable_name] = value
        return result

    # Create envfile in Dockerfile
    def import_env(self, stage):
        if 'envvars' in self.conf[stage]:
            envvars = self.conf[stage]['envvars']
            envvars = self.resolve_env_var(envvars)
            concat = ""

            for name,value in envvars.items():
                name = "{stage}_{name}".format(stage=str(stage).upper(), name=name)
                value = parse_content(value)
                export_var(name,value)
                
                if stage not in self.envvars:
                    self.envvars[stage] = OrderedDict()
                self.envvars[stage][name] = value
                concat = concat + "{name}='{value}'\n".format(name=name,value=value)

                if stage in ['general', 'build', 'setup', 'config']:
                    self.docker_envvars.append('{name}="{value}"'.format(name=name,value=value))

            export_var('DOCKERFILE_BUILDER_{stage}_ENVVARS'.format(stage=stage.upper()), concat)

    # Create ADD in Dockerfile
    def import_files(self, stage):
        if 'import' in self.conf[stage]:
            files = self.conf[stage]['import']

            concat = get_command_output('echo "$DOCKERFILE_BUILDER_IMPORTS"')
            
            if type(files) is list:
                files = list_to_dict(files)

            for key,value in files.items():
                if concat:
                    concat += "\n"
                paths = value.split(':')
                source_path = paths[0]
                build_path = paths[1]

                if source_path and build_path:
                    concat = concat + "ADD {source_path} {build_path}".format(source_path=source_path,build_path=build_path)

            export_var('DOCKERFILE_BUILDER_IMPORTS', concat)



    def import_processes(self, stage, commands_prefix="", row_prefix=""):
        processes = self.conf[stage]['processes']
        concat = ""

        for process in processes:
            concat += "# {title}\n".format(title=process['title'])

            commands = ""
            # for command in process['commands']:
            #     if 'shell_condition' in process:
            #         commands += "    {row_prefix}".format(row_prefix=row_prefix)
            #     else:
            #         commands += commands_prefix
            #     commands +=  "{command}\n".format(command=command)
            #     
            for command in process['commands']:
                if 'shell_condition' in process:
                    commands += "    {row_prefix}".format(row_prefix=row_prefix)
                    command_suffix = ';\n'
                else:
                    commands += commands_prefix
                    command_suffix = '\n'

                commands += "{command}{command_suffix}".format(
                                commands_prefix=commands_prefix,
                                command=command,
                                command_suffix=command_suffix
                            )

            if 'shell_condition' in process:
                string = "{commands_prefix}if [ {condition} ]; then\n{commands}{row_prefix}fi\n"
                concat += string.format(
                            commands_prefix=commands_prefix,
                            condition=process['shell_condition'],
                            commands=commands,
                            row_prefix=row_prefix
                        )
            else:
                concat += commands + "\n"
            #concat = os.path.expandvars(concat)

        export_var('{stage}_PROCESSES'.format(stage=stage.upper()), concat)

    def eval_template(self, template):

        template_file = GLOBALS['locations']['dockerfile-builder'] + '/' + GLOBALS['dockerfile-builder']['folders']['templates'] + '/' + GLOBALS['dockerfile-builder']['templates'][template]
        target_file = GLOBALS['locations']['pwd'] + "/" + GLOBALS['dockerfile-builder']['envvars']['paths']['target'][template]
        
        prepare_file(target_file)
        
        target_handler = open(target_file, "w");

        with open(template_file, "r") as template_file_handler:
            content = os.path.expandvars(template_file_handler.read())
            if template == "test":
                content = os.path.expandvars(content)
            target_handler.write(content)
        
        target_handler.close()
        template_file_handler.close()

        #run_command("envsubst < " + template_file, GLOBALS['locations']['pwd'] + "/" + target, '/dev/null')

    def verify_test_file(self):
        if verify_yaml_file(GLOBALS['dockerfile-builder']['envvars']['paths']['target']['test']) == False:
            print_error('Invalid travis file')
            exit(1)

        
    def __init__(self):

        # Import dockerfile-builder GLOBALS
        dir_path = os.path.dirname(os.path.realpath(__file__))
        import_globals(dir_path + "/globals.yaml")
        from Globals import GLOBALS

        # Build dockerfile-builder envvars
        self.conf = {'dockerfile_builder':GLOBALS['dockerfile-builder']}
        self.import_env('dockerfile_builder')
        self.import_files('dockerfile_builder')


    #def build(self):

        # Import build config
        build_yaml = get_path("{}/{}".format( GLOBALS['locations']['pwd'], GLOBALS['dockerfile-builder']['config']['file']['name'] ))
        self.conf = get_data_from_yaml(build_yaml,True)

        print_message('Importing enviroment variables')
        # Build env vars
        self.import_env('general')
        # Build build vars
        self.import_env('build')
        # Build setup vars
        self.import_env('setup')
        # Build config vars
        self.import_env('config')
        # Build config vars
        self.import_env('test')

        print_message('Importing files in build')

        self.import_files('build')

        print_message('Importing processes')
        # Import setup processes
        self.import_processes('setup')
        # Import config processes
        self.import_processes('config')
        # Import test processes
        self.import_processes('test', ' - ', "    ")

        # Build env file
        print_message('Building enviroment file')

        docker_env  = "ENV"
        for envvar in self.docker_envvars:
            docker_env += " \\\n\t" + envvar 
        export_var('DOCKERFILE_BUILDER_ENVVARS', docker_env)

        # ENTRYPOINT
        additional = ""
        if 'additional' in self.conf['build']['envvars']['dockerfile']['ports']:
            additional = parse_content(self.conf['build']['envvars']['dockerfile']['ports']['additional'])
        export_var('BUILD_DOCKERFILE_PORTS_ADDITIONAL', additional)

        self.eval_template('env')

        # Build setup file
        print_message('Building setup file')
        self.eval_template('setup')
        #GLOBALS['dockerfile-builder']['templates']['setup'], GLOBALS['dockerfile-builder']['target']['path']['setup'])

        # Build config file
        print_message('Building config file')
        self.eval_template('config')
        #self.eval_template(GLOBALS['dockerfile-builder']['templates']['config'], GLOBALS['dockerfile-builder']['target']['path']['config'])

        # Build docker file
        print_message('Building dockerfile')#{file}'.format(file=GLOBALS['dockerfile-builder']['target']['path']['dockerfile']))

        # ENTRYPOINT (deprecated)
        #entrypoint = ""
        #if 'entrypoint' in self.conf['build']['envvars']['dockerfile']:
        #    entrypoint = 'ENTRYPOINT ["{entrypoint}"]'.format(entrypoint=self.conf['build']['envvars']['dockerfile']['entrypoint'])
        #export_var('DOCKERFILE_BUILDER_ENTRYPOINT', entrypoint)

        # CMD
        cmd=""
        if 'cmd' in self.conf['build']['envvars']['dockerfile']:
            cmd='CMD ["{config_script} && {cmd}"]'.format(
                config_script=GLOBALS['dockerfile-builder']['envvars']['paths']['dockerfile']['config'],
                cmd=self.conf['build']['envvars']['dockerfile']['cmd']
            )
        export_var( 'DOCKERFILE_BUILDER_CMD', cmd )

        self.eval_template('dockerfile')#GLOBALS['dockerfile-builder']['templates']['dockerfile'], GLOBALS['dockerfile-builder']['target']['path']['dockerfile'])

        # Build test file
        print_message('Building travis file')

        self.eval_template('test')

        print_message('Verify travis file')

        self.verify_test_file()

        print_success("Completed")

# ---------

# Init
builder = Builder()