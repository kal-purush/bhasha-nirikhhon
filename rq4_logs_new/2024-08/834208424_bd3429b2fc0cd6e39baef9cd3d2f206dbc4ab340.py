"""Class to enable accessing azure tables

Initialised with a specific table name

Author: Panu Hietanen
Date: 06/08/2024
"""

from azure.data.tables import TableServiceClient, TableEntity, UpdateMode
from azure.core.exceptions import AzureError
from typing import Optional, List, Dict, Any, Union
from typing_extensions import Self
import os
from azure.core.paging import ItemPaged

import sys
import logging

class Table:
    """Table class for Azure Table Storage operations."""

    def __init__(self: Self, table_name: str):
        self.logger = logging.getLogger('azure')
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler(stream=sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.table_name = table_name
        self.conn_str = os.getenv("CONNECTION_STRING")
        if not self.conn_str:
            self.logger.error("Connection string not found in environment variables.")
            raise ValueError("Connection string not found")

        # Create service client, used to access specific tables
        try:
            self.service_client = TableServiceClient.from_connection_string(conn_str=self.conn_str)
            self.properties = self.service_client.get_service_properties()
            self.logger.info("TableServiceClient initialized successfully.")
            self.logger.info(f"Table properties: {self.properties}")
        except AzureError as e:
            self.logger.error(f"Error initializing TableServiceClient: {e}")
            raise

        # Use service client to access table specified on object creation. Service client should not be used after this point
        try:
            self.client = self.service_client.get_table_client(table_name=self.table_name)
            self.logger.info(f"Table client for '{self.table_name}' retrieved successfully.")
        except AzureError as e:
            self.logger.error(f"Error getting table client for {self.table_name}: {e}")
            raise

    def create_entity(self: Self, partition: str, row: str, data: Dict[str, Any]) -> None:
        """Create an entity within a table for a given partition and row"""
        new_entity = {
            'PartitionKey': partition,
            'RowKey': row,
            **data,
        }
        try:
            self.client.create_entity(entity=new_entity)
            self.logger.info(f"Entity created in table {self.table_name}.")
        except AzureError as e:
            self.logger.error(f"Error creating entity in table {self.table_name}: {e}")

    def update_with_dict(self: Self, old: TableEntity, data: Dict[str, Any]) -> None:
        """Update an entity using a dictionary of new data
        
        Parameters
        ---
        old: TableEntity, can be returned using `get_entity()`
        data: Dictionary of new data to be merged
        """
        new_dict = {
            'PartitionKey': old["PartitionKey"],
            'RowKey': old["RowKey"],
            **data,
        }
        self.update(new_dict)
    
    def update(self: Self, data: Union[TableEntity, Dict[str, Any]]) -> None:
        """Update an entity"""
        try:
            self.client.update_entity(mode=UpdateMode.MERGE, entity=data)
            self.logger.info(f"Successfully updated entity at ({data['PartitionKey']}, {data['RowKey']})")
        except AzureError as e:
            self.logger.error(f"Error updating entity ({data['PartitionKey']}, {data['RowKey']}) in table {self.table_name}: {e}")


    def get_data(self: Self, filters: str) -> Optional[ItemPaged[TableEntity]]:
        """Get data that matches filters
        
        Returns
        ---
        List of dictionaries that match filters, or `None` if error is raised.
        """
        try:
            entities = self.client.query_entities(query_filter=filters)
            self.logger.info(f"Entities retrieved successfully from table {self.table_name}.")
            return list(entities)
        except AzureError as e:
            self.logger.error(f"Error querying entities in table {self.table_name}: {e}")
            return None
    
    def get_entity(self: Self, partition: str, row: str) -> Optional[TableEntity]:
        """Get data for given partition and row
        
        Returns
        ---
        A dictionary corresponding to the given values, or `None` if the partition and key pair don't exist.
        """
        try:
            entity = self.client.get_entity(partition_key=partition, row_key=row)
            self.logger.info(f"Entity retrieved successfully from table {self.table_name}.")
            return entity
        except AzureError as e:
            self.logger.error(f"Error getting entity from table {self.table_name}: {e}")
            return None
        
    def delete_entity(self: Self, partition: str, row: str) -> None:
        """Delete an entity from the table"""
        try:
            self.client.delete_entity(partition_key=partition, row_key=row)
            self.logger.info(f"Entity at ({partition}, {row}) deleted from table {self.table_name}.")
        except AzureError as e:
            self.logger.error(f"Error deleting entity ({partition}, {row}) in table {self.table_name}: {e}")

    def mark_as_reviewed(self: Self, partition: str, row: str) -> None:
        """Mark an observation as reviewed"""
        try:
            entity = self.get_entity(partition, row)
            if entity:
                entity['reviewed'] = True
                self.update(entity)
                self.logger.info(f"Marked entity at ({partition}, {row}) as reviewed.")
            else:
                self.logger.error(f"Entity at ({partition}, {row}) not found.")
        except AzureError as e:
            self.logger.error(f"Error marking entity as reviewed in table {self.table_name}: {e}")