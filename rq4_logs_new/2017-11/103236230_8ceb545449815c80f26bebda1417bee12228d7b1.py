import json
import logging
import re
import traceback

def _parse_operation(operation):
    """
    Given an operation string, method returns the name of the operation 
    and a tuple of the parsed operations. Supported operations are
    access: Match any character that is not { or [
    filter: Must be enclosed between {}. Within braces should match format
        <string><operator><string>. Operator should match 
        ==, !=, >, >=, <, <=
    index: Must be enclosed between []. Within brackets should match any 
        digit or * character
        
    :param operation: Operation string 
    :type param: str
    """
    logging.info("Parsing operation: {}".format(operation))
    access_pattern = "^([^{[]+)$"
    filter_pattern = "^\{([^!=><]+)(\=\=|\!\=|\>|\>\=|\<|\<\=)([^!=><]+)\}$"
    index_pattern = "^\[(\d+|\*)\]$"
    access_regex = re.compile(access_pattern)
    filter_regex = re.compile(filter_pattern)
    index_regex = re.compile(index_pattern)
    # Matching operation string against the three operation types 
    match_object = re.match(access_regex, operation)
    if match_object:
        logging.debug("Access operation")
        return 'access', match_object.groups()
    match_object = re.match(filter_regex, operation)
    if match_object:
        logging.debug("Filter operation")
        return 'filter', match_object.groups()
    match_object = re.match(index_regex, operation)
    if match_object:
        logging.debug("Index operation")
        return 'index', match_object.groups()
    logging.warn("Unknown operation type")       
    return None, None

def perform_operations_on_json(json_object, operations):
    """
    Perform operations on json and return the result.
    Operations is a sequential set of operation seperated by a dot. 
    Operation is one of 3 types - access, filter and index. 
    
    Operation Types:
    Access operation is specified as a string and returns a matching 
    object from dict
    
    Filter operation is specified in {<string><operator><string>} format.
    <operator> should match ==, !=, >, >=, <, <=. Operates on a dict and 
    returns the dict as is if the filter condition is met
    
    Index operation is specified in  [<index>] format. <index> can be a 
    number or *. When number is specified it returns the object at that 
    index location. When star is specified it de-nests a list i.e. passes
    through each list item and returns the objects in the list.
    
    E.g. input_json = { 'name':  {'first':'Dilly', 'last':'Berty'},
                        'phone': [ 
                                 {'area':111, 'number':222333}, 
                                 {'area':444, 'number':555666},
                                 {'area':'aaa', 'number':'bbbccc'}
                                 ]
                        }
    perform_operations_on_json(input_json, 'name')
    {'first': 'Dilly', 'last': 'Berty'}
    
    perform_operations_on_json(input_json, 'name.first')
    'Dilly'
    
    perform_operations_on_json(input_json, 'phone.{area=="aaa"}')
    None
    
    perform_operations_on_json(input_json, 'phone.[*].{area=="aaa"}')
    {'area': 'aaa', 'number': 'bbbccc'}
    
    perform_operations_on_json(input_json, 'phone.[*].{area!=111}')
    [{'number': 555666, 'area': 444}, {'number': 'bbbccc', 'area': 'aaa'}]
    
    perform_operations_on_json(input_json, 'phone.[*].{area!=111}.[0]')
    {'number': 555666, 'area': 444}
    
    :param json_object: JSON Object
    :type json_object: Python object
    :param operations:Operation string
    :type operations: str
    """
    if not json_object:
        logging.warn("JSON data not provided")
        return None
    if not operations:
        logging.warn("Operations str not specified")
        return None
    stack = list()
    stack.append(json_object)
    try:
        for operation in operations.split('.'):
            stack_buffer = list()
            operation_type, operator = _parse_operation(operation)
            logging.debug("Processing JSON: {}".format(stack))
            if operation_type == 'access':
                logging.debug("Performing access operation")
                namespace = operator[0]
                for stack_item in stack:
                    stack_item_value = stack_item.get(namespace)
                    if stack_item_value is not None:
                        stack_buffer.append(stack_item_value)
            elif operation_type == 'filter':
                logging.debug("Performing filter operation")
                namespace, condition, value = operator
                for stack_item in stack:
                    filter_value = stack_item.get(namespace)
                    eval_string = "{}{}{}".format("filter_value",
                                                  condition,
                                                  value)
                    logging.debug("Evaluating {}{}{}"
                                  .format(filter_value,
                                          condition,
                                          value))
                    if eval(eval_string):
                        stack_buffer.append(stack_item)
            elif operation_type == 'index':
                logging.debug("Performing index operation")
                index = operator[0]
                if index == "*":
                    logging.debug("Received * index. Stacking all "+
                                  "entries within list")
                    for stack_item in stack:
                        if isinstance(stack_item, list):
                            for item in stack_item:
                                stack_buffer.append(item)
                elif index.isdigit():
                    stack_buffer.append(stack[int(index)])
            else:
                logging.warn("Unknown operation type. "+
                             "Cannot process further")
                return None
            stack = stack_buffer
            logging.debug("Processed JSON: {}".format(stack))
        # If result has a single item then return that item 
        # else return the list of items 
        if len(stack) == 0:
            return None
        if len(stack) == 1:
            return stack[0]
        else:
            return stack
    except Exception as e:
        logging.warn("Exception while processing json")
        traceback.print_exc()
        return None

def perform_multiple_operations_on_json(json_object, operations_map):
    """
    Return a dictionary with name and output from performing corresponding 
    operations on json_object. See perform_operations_on_json 
    documentation for more details
    
    E.g. perform_multiple_operations_on_json(
    input_json, 
    {'first_name': 'name.first', 'phone': 'phone.[*].[0]'}
    )
    {'first_name': 'Dilly', 'phone': {'area': 111, 'number': 222333}}
    
    :param json_object: JSON Object
    :type json_object: Python object
    :param operations_map: Mapping of name and operations. 
    E.g. {'first_name': 'name.first', 'phone': 'phone.[*].[0]'}
    :param operations_map: dict
    """
    output = {}
    for name, operations in operations_map.iteritems():
        output[name] = perform_operations_on_json(json_object,
                                                  operations=operations)
    return output