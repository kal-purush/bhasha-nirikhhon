import jmespath
from string import Template


class ObjectDotAccessWrapper:
    def __init__(self, _data):
        self.data = _data
        if isinstance(_data, (dict, list, tuple)):
            self.__recurse_flat(self, _data)

    def __repr__(self):
        return str(self.data)

    def __recurse_flat(self, _object, _dict):
        for key in _dict:
            if isinstance(_dict[key], dict):
                _object_child = ObjectDotAccessWrapper(_dict[key])
                setattr(_object, key, _object_child)
                # ic("nested key", key)
                self.__recurse_flat(_object_child, _dict[key])
            elif isinstance(_dict[key], (list, tuple)):
                setattr(_object, key, _dict[key])
                for index, value in enumerate(_dict[key]):
                    if isinstance(value, (dict, list, tuple)):
                        _object_child = ObjectDotAccessWrapper(value)
                        # setattr(_object, f"{key}i{index}", _object_child)
                        _dict[key][index] = _object_child
                        # ic("nested key", key)
                        # self.__recurse_flat(_object_child, value)
                    # else:
                    #     setattr(_object, f"{key}i{index}", value)
            else:
                setattr(_object, key, _dict[key])
        return _object


def __replace_parameters(parameters_dict, module_object_dict):
    for parameter_key in parameters_dict:
        if parameters_dict[parameter_key]:
            val_type = type(parameters_dict[parameter_key])
            value = str(parameters_dict[parameter_key])
            # ic(parameter_key, value)
            template = Template(value)
            value = template.safe_substitute(module_object_dict)
            parameters_dict[parameter_key] = value
            # ic(parameter_key, value)


def __compose_compare_express(testcase_dict, full_message):
    compare_operator, compare_values = testcase_dict.popitem()
    compare_operator = compare_operator.lower()
    fact_express = compare_values[0]
    fact_value = jmespath.search(fact_express, full_message)
    excepted_value = compare_values[1]
    """
        lt：less than 小于
        le：less than or equal to 小于等于
        eq：equal to 等于
        ne：not equal to 不等于
        ge：greater than or equal to 大于等于
        gt：greater than 大于
    """
    operator_dict = {"eq": "==", "lt": "<", "gt": ">", "le": "<=", "ge": ">=", "ne": "!="}
    if compare_operator in operator_dict:
        if isinstance(excepted_value, str):
            return f"'{fact_value}' {operator_dict[compare_operator]} '{excepted_value}'"
        else:
            return f"{fact_value} {operator_dict[compare_operator]} {excepted_value}"
    else:
        raise KeyError(f"{compare_operator} is not supported")
