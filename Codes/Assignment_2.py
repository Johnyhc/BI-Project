import json
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from jsonpath_ng import parse
from enum import Enum


class Interface(ABC):

    @abstractmethod
    def get_data_by_field(self, field_name):
        """Fetch the data by given feild name """

    @abstractmethod
    def get_data_by_id(self, id):
        """Fetch the data by given ID  """

    @abstractmethod
    def get(self):
        """Fetch all data """


class TransformMask(Enum):
    # add here any masks you want
    CLEAN_STRING = ".strip().lower()"
    CAPITAL_LETTER = ".strip().lower().title()"


class Database:
    def __init__(self):
        self.db = {
            "source": [],
            "destination": [],
            "transform": [],
            "mapping": []
        }

        self.add_source("1", "date", "$.date", "str", True)
        self.add_source("2", "id", "$.id", "str", True)
        self.add_source("3", "start", "$.start", "str", True)
        self.add_source("4", "num_customers", "$.num_customers", "int", True)
        self.add_source("5", "sitting_time", "$.sitting_time", "int", True)
        self.add_source("6", "waiter", "$.waiter", "str", True)
        self.add_source("7", "day", "$.day", "int", True)
        self.add_source("8", "end", "$.end", "str", True)

        self.add_destination("1", "date", "date", "str", "Fact")
        self.add_destination("2", "id", "id", "str", "Fact")
        self.add_destination("3", "start", "start", "str", "Fact")
        self.add_destination("4", "num_customers", "num_customers", "float", "Fact")
        self.add_destination("5", "sitting_time", "sitting_time", "float", "Fact")
        self.add_destination("6", "waiter", "waiter", "str", "Fact")
        self.add_destination("7", "day", "day", "float", "Fact")
        self.add_destination("8", "end", "end", "str", "Fact")

        self.add_transform("1", 'CAPITAL_LETTER')
        self.add_transform("2", 'CLEAN_STRING')

        self.add_mapping("1", "1", "1", "", "Fact")
        self.add_mapping("2", "2", "2", "", "Fact")
        self.add_mapping("3", "3", "3", "", "Fact")
        self.add_mapping("4", "4", "4", "", "Fact")
        self.add_mapping("5", "5", "5", "", "Fact")
        self.add_mapping("6", "6", "6", "", "Fact")
        self.add_mapping("7", "7", "7", "", "Fact")
        self.add_mapping("8", "8", "8", "", "Fact")

    def add_source(self, id, field_name, field_mapping, field_type, is_required):
        self.db["source"].append({
            "id": id,
            "source_field_name": field_name,
            "source_field_mapping": field_mapping,
            "source_field_type": field_type,
            "source_is_required": is_required,
        })
        pass

    def add_destination(self, id, field_name, field_mapping, field_type, table):
        self.db["destination"].append({
            "id": id,
            "destination_field_name": field_name,
            "destination_field_mapping": field_mapping,
            "destination_field_type": field_type,
            "default_value": "n/a",
            "destination_table": table
        })

    def add_transform(self, id, mask):
        self.db["transform"].append({
            "id": id,
            "transform_mask": mask
        })

    def add_mapping(self, id, source, destination, transform, table):
        self.db["mapping"].append({
            "id": id,
            "mapping_source": source,
            "mapping_destination": destination,
            "mapping_transform": transform,
            "destination_table": table
        })

    @property
    def get_data_source_target_mapping(self):
        return self.db


"""### Source class

Inherited from Interface for the common methods and from Database for common variables
"""


class Source(Interface, Database):
    def __init__(self):
        Database.__init__(self)

    # should be implemented - inherited from Interface
    def get_data_by_field(self, field_name):
        data = self.get
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None
    @property
    def get(self):
        return self.get_data_source_target_mapping.get("source")

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id") == self.id:
                return x
        return None


"""### Target class

Inherited from Interface for the common methods and from Database for common variables
"""


class Target(Interface, Database):

    def __init__(self):
        Database.__init__(self)

    # should be implemented - inherited from Interface
    def get_data_by_field(self, field_name):
        data = self.get
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None

    @property
    def get(self):
        return self.get_data_source_target_mapping.get("destination")

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id").__str__() == self.id.__str__():
                return x
        return None


"""### Transform Class

Inherited from Interface for the common methods and from Database for common variables
"""


class Transform(Interface, Database):

    def __init__(self):
        Database.__init__(self)

    # should be implemented - inherited from Interface
    def get_data_by_field(self, field_name):
        data = self.get
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None

    @property
    def get(self):
        return self.get_data_source_target_mapping.get("transform", [])

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id").__str__() == self.id.__str__():
                return x
        return None


"""### Mapping class

Inherited from Interface for the common methods and from Database for common variables
"""


class Mappings(Interface, Database):

    def __init__(self):
        Database.__init__(self)

    @property
    def get(self):
        return self.get_data_source_target_mapping.get("mapping")

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id").__str__() == self.id.__str__():
                return x
        return None

    def get_data_by_field(self, field_name):
        return None


"""### Format Class - JSON

Search the source data value inside a JSON file 
"""


class JsonQuery:
    def __init__(self, json_path, json_data):
        self.json_path = json_path
        self.json_data = json_data

    def get(self):
        jsonpath_expression = parse(self.json_path)
        match = jsonpath_expression.find(self.json_data)
        source_data_value = match[0].value
        return source_data_value

    def __str__(self):
        return self.get()



"""### Combine it All - STTM"""


class STTM:
    def __init__(self, input_json):
        self.input_json = input_json
        self.mapping_instance = Mappings()
        self.source_instance = Source()
        self.destination_instance = Target()
        self.transform_instance = Transform()
        self.look_up_mask = {i.name: i.value for i in TransformMask}
        self.json_data_transformed = {}
        self.to_table = {}

    def _get_mapping_data(self):
        return self.mapping_instance.get

    def _get_mapping_source_data(self):
        return self.source_instance.get

    def get_transformed_data(self):

        for mappings in self._get_mapping_data():

            """fetch the source mapping """
            mapping_source_id = mappings.get("mapping_source")
            mapping_destination_id = mappings.get("mapping_destination")
            mapping_transform_id = mappings.get("mapping_transform")
            mapping_table = mappings.get("destination_table")

            mapping_source_data = self.source_instance.get_data_by_id(id=mapping_source_id)
            transform_data = self.transform_instance.get_data_by_id(id=mapping_transform_id)

            """Fetch Source  field Name"""
            source_field_name = mapping_source_data.get("source_field_name")

            """if field given is not present incoming json """
            if source_field_name not in self.input_json.keys():
                if mapping_source_data.get("is_required"):
                    raise Exception(
                        "Alert ! Field {} is not present in JSON please FIX mappings ".format(source_field_name))
                else:
                    pass

            else:
                source_data_value = JsonQuery(
                    json_path=mapping_source_data.get("source_field_mapping"),
                    json_data=self.input_json
                ).get()

                """check the data type for source if matches with what we have """
                if mapping_source_data.get("source_field_type") != type(source_data_value).__name__:
                    if source_data_value is not None:
                        _message = (
                            "Alert ! Source Field :{} Datatype has changed from {} to {} ".format(source_field_name,
                                                                                                  mapping_source_data.get(
                                                                                                      "source_field_type"),
                                                                                                  type(
                                                                                                      source_data_value).__name__))
                        print(_message)
                        raise Exception(_message)

                """Query and fetch the Destination | target """
                destination_mappings_json_object = self.destination_instance.get_data_by_id(
                    id=mappings.get("mapping_destination"))

                destination_field_name = destination_mappings_json_object.get("destination_field_name")
                destination_field_type = destination_mappings_json_object.get("destination_field_type")
                self.to_table[destination_field_name] = mapping_table

                dtypes = [str, float, list, int, set, dict]

                for dtype in dtypes:

                    """Datatype Conversion """
                    if destination_field_type == str(dtype.__name__):

                        """is source is none insert default value"""
                        if source_data_value is None:
                            self.json_data_transformed[destination_field_name] = dtype.__call__(
                                destination_mappings_json_object.get("default_value")
                            )

                        else:
                            """check if you have items to transform"""
                            if transform_data is not None:
                                """ check for invalid mask name """
                                if transform_data.get("transform_mask") not in list(self.look_up_mask.keys()):
                                    raise Exception(
                                        f"Specified Transform {transform_data.get('transform_mask')} is not available please select from following Options :{list(self.look_up_mask.keys())}")
                                else:
                                    mask_apply = self.look_up_mask.get(transform_data.get("transform_mask"))
                                    converted_dtype = dtype.__call__(source_data_value)
                                    mask = f'converted_dtype{mask_apply}'
                                    curated_value = eval(mask)
                                    self.json_data_transformed[destination_field_name] = curated_value

                            else:
                                self.json_data_transformed[destination_field_name] = dtype.__call__(source_data_value)

        return self.json_data_transformed, self.to_table


data = json.load(open("./json_data/tables.json"))
transformed_data = []
for item in data:
    helper = STTM(input_json=item)
    response, mapping = helper.get_transformed_data()
    transformed_data.append(response)
    print(response)
print(mapping)

df = pd.DataFrame(transformed_data)