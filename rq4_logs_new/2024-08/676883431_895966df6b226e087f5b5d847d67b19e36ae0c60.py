# coding = utf-8
# -*- coding: utf-8 -*-

import os
import pandas
import frictionless

from ..project_management import ContainerHandler, ContainerHandlerFactory, Pointer, Crawler

class TableContainer(ContainerHandler):
    containerType = 'table'
    containerFormat = "Table"

    # dictionary of DB fields needed to save this subclass instance attributes
    DBfields = {"line_delimiter": "text",
                "cell_delimiter": "datetime",
                "decimal_separator": "datetime",
                "encoding": "text"}

    # dictionary of attribute names to be used for DB save/update - current values need to be obtained at right time before saving
    serializationDict = {"line_delimiter": "lineDelimiter",
                         "cell_delimiter": "cellDelimiter",
                         "decimal_separator": "decimalSeparator"}

    def __init__(self, project_manager, parent_container, **kwargs):
        super().__init__(project_manager, parent_container, **kwargs)
        self.lineDelimiter = kwargs.get("line_delimiter")
        self.cellDelimiter = kwargs.get("cell_delimiter")
        self.decimalSeparator = kwargs.get("decimal_separator")


        pass

class ColumnContainer(ContainerHandler):
    containerType = 'column'
    containerFormat = "Table column"
    def __init__(self, project_manager, parent_container, **kwargs):
        super().__init__(project_manager, parent_container, **kwargs)
        self.unit = kwargs.get("unit")
        self.method = kwargs.get("method")
        pass