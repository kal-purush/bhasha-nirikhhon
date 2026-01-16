import json
from typing import Callable, List

import faker
import faker.providers
from jinja2 import Environment, FunctionLoader

from oarepo_model_builder.builders import OutputBuilder
from oarepo_model_builder.builders.utils import ensure_directory
from oarepo_model_builder.templates import templates

from ..builder import ModelBuilder
from ..builders import process
from ..builders.json_base import JSONBaseBuilder
from ..entrypoints import load_entry_points_list
from ..property_preprocessors import PropertyPreprocessor
from deepmerge import always_merger


SKIP = "skip"


class InvenioLayoutBuilder(JSONBaseBuilder):
    TYPE = "script_sample_data"
    output_file_type = "layout"
    output_file_name = "layout"
    parent_module_root_name = "jsonschemas"

    def __init__(self, builder: ModelBuilder, property_preprocessors: List[PropertyPreprocessor]):
        super().__init__(builder, property_preprocessors)
        self.generator = SampleDataGenerator()
        self.ui = {}
        self.ui = self.builder.schema.schema['oarepo:ui'] #top oarepo:ui

        self.data = {}
        self.collected = []


    @process("/model/**", condition=lambda current, stack: stack.schema_valid)
    def model_element(self):
        schema_element_type = self.stack.top.schema_element_type

        if schema_element_type == "property":
            key = self.process_path(self.stack.path, self.stack.top.key)
            self.generate_property(key)
        elif schema_element_type == "items":
            pass
        else:
            if 'oarepo:ui' in self.stack.top.data and self.stack.path not in self.collected:
                self.collected.append(self.stack.path)
                key = self.process_path(self.stack.path, self.stack.top.key)
                for element in self.ui:
                    if element in self.stack.top.data['oarepo:ui']:
                        content = self.stack.top.data['oarepo:ui'][element]
                        self.ui[element] = self.merge_content(self.ui[element], key, content)
                        print(self.ui)

            self.build_children()

    def process_path(self, path, key):
        path_array = path.split('/')
        path_array = path_array[4:]
        if len(path_array)> 0:
            path_array = path_array[:-1]
        full_key = ''
        for p in path_array:
            if p == 'properties':
                continue
            if full_key == '':
                full_key = p
            else:
                full_key = full_key + '.' + p
        if full_key != '':
            full_key = full_key + '.'
        full_key = full_key + key
        return full_key
    def generate_property(self, key):
        if not self.skip(self.stack):
            if "properties" in self.stack.top.data:
                if 'oarepo:ui' in self.stack.top.data and self.stack.path not in self.collected:
                    self.collected.append(self.stack.path)
                    self.stack.top.data['oarepo:ui'] = self.update_datafield(self.stack.top.data['oarepo:ui'], key, self.stack.top.data.properties)

                    for element in self.ui:
                        if element in self.stack.top.data['oarepo:ui']:
                            content = self.stack.top.data['oarepo:ui'][element]
                            self.ui[element] =self.merge_content(self.ui[element], key, content)
                            print(self.ui)
                    self.output.merge(self.ui)
                self.output.enter(key, {})
                self.build_children()
                self.output.leave()
            elif "items" in self.stack.top.data:
                self.output.enter(key, [])
                self.build_children()
                top = self.output.stack.real_top

                # make items unique, just for sure
                top_as_dict = {}
                for t in top:
                    top_as_dict[json.dumps(t, sort_keys=True)] = t
                top.clear()
                top.extend(top_as_dict.values())

                self.output.leave()
            else:
                if 'oarepo:ui' in self.stack.top.data and self.stack.path not in self.collected:
                    self.collected.append(self.stack.path)
                    self.stack.top.data['oarepo:ui'] = self.update_datafield(self.stack.top.data['oarepo:ui'], key)
                    for element in self.ui:
                        if element in self.stack.top.data['oarepo:ui']:
                            content = self.stack.top.data['oarepo:ui'][element]
                            self.ui[element] =self.merge_content(self.ui[element], key, content)
                            print(self.ui)
                    self.output.merge(self.ui)


    def update_datafield(self, dictionary, path, properties = None):
        for layout in dictionary:
            if 'dataField' in dictionary[layout] and dictionary[layout]['dataField'] == "":
                dictionary[layout]['dataField'] = path

        if properties:
            for property in properties:
                dictionary = json.loads(json.dumps(dictionary).replace(property, path + '.' + property))


        return dictionary

    def merge_content(self, dictionary, path, content):
        content = json.dumps(content)
        d = json.dumps(dictionary).replace(path, str(json.loads(content)))

        d = d.replace('"{', '{')
        d = d.replace('}"', '}')
        d = d.replace('"[', '[')
        d = d.replace(']"', ']')
        d = d.replace('\'', '"')

        d = json.loads(d)
        return d


    def build(self, schema):
        output_name = schema.settings[self.output_file_name]
        output = self.builder.get_output(self.output_file_type, output_name)
        if not output.created:
            return

        super().build(schema)


    def skip(self, stack):
        return get_oarepo_ui(stack).get("skip", False)

    def on_enter_model(self, output_name):
        self.output.next_document()


def nested_replace( structure, original, new ):
    if type(structure) == list:
        return [nested_replace( item, original, new) for item in structure]

    if type(structure) == dict:
        return {key : nested_replace(value, original, new)
                     for key, value in structure.items() }

    if structure == original:
        return new
    else:
        return structure










