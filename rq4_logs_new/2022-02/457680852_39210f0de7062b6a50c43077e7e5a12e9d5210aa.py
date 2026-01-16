import shutil
import argparse
import sys
import logging
import yaml
import yaml.loader
from icecream import ic
from jinja2 import Environment, select_autoescape, FileSystemLoader
from os import scandir, DirEntry
import os
from httprunner3_common import HttpRunner3Common
from httprunner3_logging import logger

#  ic.disable()


class HttpRunner3(object):
    def __init__(self, __obj_common):
        # self._cli_parameters_dict = __cli_parameters_dict
        self._obj_common = __obj_common
        self._verbose_mode = __obj_common.cli_parameters["verbose"]

    def make_one_file(self, _source_file_path, _target_file_path):
        logger.debug(f"load source yaml file:{_source_file_path}")
        whole_config = self._obj_common.load_yaml(_source_file_path)
        if not whole_config.get("config", None):
            _msg = f"config section is missing. source: {_source_file_path}"
            logger.error(_msg)
            raise SystemExit(_msg)
        if not whole_config.get("teststeps", None):
            _msg = f"teststeps section is missing. source: {_source_file_path}"
            logger.error(_msg)
            raise SystemExit(_msg)

        whole_config["source_yaml_path"] = _source_file_path
        whole_config["target_test_path"] = _target_file_path
        # ic(whole_config)
        logger.debug(f"open target pytest file:{_target_file_path}")
        target_fp = open(_target_file_path, mode="w", encoding="utf-8")
        self.make_one_step(whole_config, target_fp)
        target_fp.close()
        # ic(target_fp)
        # ic(dir(target_fp))

    def make_one_step(self, _whole_config, _target_fp):
        template = self._obj_common.load_template("config_section.py")
        base_url = _whole_config["config"].get("base_url", "")
        config_content = template.render(base_url=base_url)
        # ic(config_content)
        _target_fp.write(config_content)

        # ic(_whole_config["steps"])
        steps_dict = _whole_config.get("teststeps", [])
        if len(steps_dict) == 0:
            _msg = f"at least need one teststeps: {_whole_config}"
            logger.error(_msg)
            raise SystemExit(_msg)
        for step_no, step in enumerate(steps_dict):
            if step.get("include", None):
                include_yaml_path = "{}{}{}".format(os.path.dirname(_whole_config["source_yaml_path"]),
                                                    os.path.sep, step["include"])
                # ic(include_yaml_path)
                if not self._obj_common.yaml_is_testcase_component(os.path.basename(include_yaml_path)):
                    _msg = f"is not a valid component file: {include_yaml_path}"
                    logger.error(_msg)
                    raise SystemExit(_msg)

                logger.debug(f"load source yaml component file:{include_yaml_path}")
                refer_content_dict = self._obj_common.load_yaml(include_yaml_path)
                # ic(refer_content_dict)
                step.update(refer_content_dict)
                # ic(step)

            data_base_dict = {}
            alias_base_data_dict = {}
            self._obj_common.request_data(data_base_dict, alias_base_data_dict, step.get("data_base", {}))
            # ic(data_base_dict)
            # ic(alias_base_data_dict)

            requests_dict = step.get("httprequests", [])
            if len(requests_dict) == 0:
                _msg = f"at least need one http httprequests: {step}"
                logger.error(_msg)
                raise SystemExit(_msg)
            # ic(cases_dict)
            for request_no, request in enumerate(requests_dict):
                data_dict = {}
                alias_data_dict = {}
                self._obj_common.request_data(data_dict, alias_data_dict, request.get("data", {}), is_data_base=False)
                # ic(data_dict)
                # ic(alias_data_dict)
                # ic(request)

                request_data = dict(data_base_dict.items())
                # ic(request_data)
                for key in alias_data_dict:
                    request_data[alias_base_data_dict[key]] = alias_data_dict[key]
                request_data.update(data_dict)
                # ic(request_data)
                headers = request.get("headers", {})
                headers.update(_whole_config["config"].get("headers", {}))

                template = self._obj_common.load_template("step_section.py")
                # ic(request)
                request_name = "http_request"
                # ic(request_name)

                # module_name = f"http_request_{step_no + 1}_{case_no + 1}"
                request_name = f"{request_name}_{step_no + 1}_{request_no + 1}"
                case_content = template.render(request_name=request_name, request_method=step["method"],
                                               request_data=request_data, request_url=step["url"],
                                               request_headers=headers,
                                               extract_list=request.get("extract", {}),
                                               variables=step.get("variables", {}))
                # ic(case_content)
                template = self._obj_common.load_template_from_string(case_content)
                case_content = template.render()

                _target_fp.write(case_content)

                testcases_dict = request.get("testcases", [])
                if len(testcases_dict) == 0:
                    _msg = f"at least need one testcase: {request}"
                    logger.error(_msg)
                    raise SystemExit(_msg)
                for testcase_no, testcase in enumerate(testcases_dict):
                    # testcase_name = f"test_{module_name}_{validate_no + 1}"
                    # testcase_name = f"{module_name}_{validate_no + 1}"
                    testcase_name = f"testcase_{step_no + 1}_{request_no + 1}_{testcase_no + 1}"
                    template = self._obj_common.load_template("testcase_section.py")
                    # ic(testcase)
                    testcase_content = template.render(request_name=request_name, testcase_name=testcase_name,
                                                       testcase=testcase)
                    # ic(testcase_content)
                    _target_fp.write(testcase_content)

    def make_file_or_dir(self):
        # ic(cli_parameters_dict)
        _yaml_path = self._obj_common.cli_parameters["yaml"]
        _testcases_path = self._obj_common.cli_parameters["testcases"]
        _need_clean = self._obj_common.cli_parameters["clean"]
        work_dir = os.path.abspath(os.path.curdir)
        logger.debug(ic(work_dir)) if self._verbose_mode else ic(work_dir)

        if not os.path.exists(_yaml_path):
            # ic(yaml_path)
            _msg = f"source yaml file or dir is not exists: {_yaml_path} "
            logger.error(_msg)
            raise SystemExit(_msg)

        if os.path.isdir(_yaml_path):
            if os.path.exists(_testcases_path) and os.path.isdir(_testcases_path):
                if _need_clean:
                    shutil.rmtree(_testcases_path)
                files = {}
                for file in list(self._obj_common.yaml_discover(_yaml_path, len(_yaml_path))):
                    target_file_name = f"{file[:file.rfind('.')]}.py"
                    target_file_path = "{}{}{}".format(_testcases_path, os.path.sep, target_file_name)
                    file = "{}{}{}".format(_yaml_path, os.path.sep, file)
                    files[file] = target_file_path
            else:
                # ic(testcases_path)
                _msg = f"testcases dir must be a exists: {_testcases_path} "
                logger.error(_msg)
                raise SystemExit(_msg)

        elif os.path.isfile(_yaml_path) and self._obj_common.yaml_is_testcase(os.path.basename(_yaml_path)):
            if self._obj_common.file_is_pytest(os.path.basename(_testcases_path)):
                files = {_yaml_path: _testcases_path}
            else:
                # ic(testcases_path)
                _msg = f"testcases value is not a  pytest file: {_testcases_path} "
                logger.error(_msg)
                raise SystemExit(_msg)
        else:
            _msg = f"invalid yaml file name: {_yaml_path} "
            logger.error(_msg)
            raise SystemExit(_msg)
        # ic(files)
        # ic(yaml_path)

        for file in files:
            target_file_path = files[file]
            logger.debug(f"make {file} -> {target_file_path}")
            target_dir_path = os.path.dirname(target_file_path).strip()
            if not target_dir_path=="" and not os.path.exists(target_dir_path):
                os.makedirs(target_dir_path, exist_ok=True)
            self.make_one_file(file, target_file_path)
            # ic(file, target_file_path)
            logger.debug(ic(file, target_file_path))


if __name__ == "__main__":
    cli = " ".join(sys.argv)
    # ic(cli)
    logger.debug(cli)
    obj_common = HttpRunner3Common()
    cli_parameters_dict = obj_common.cli_parameters
    obj_runner = HttpRunner3(obj_common)

    if cli_parameters_dict["make"]:
        logger.debug("start make ...")
        obj_runner.make_file_or_dir()
        logger.debug("make finished.")
    elif cli_parameters_dict["verify"]:
        logger.debug("start verify ...")
        obj_common.verify_yaml_or_toml_file()
        logger.debug("verify finished.")
    else:
        __msg = f"invalid cli parameters: {cli_parameters_dict} "
        logger.error(__msg)
        raise SystemExit(__msg)