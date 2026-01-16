import argparse
import asyncio
from typing import TypeAlias

from d2d.plugins.neo4j.document_to_db import DocumentToNeo4J
from d2d.tasks.document_composer import DocumentComposer

IS_COMPLETED: TypeAlias = bool


class DocumentToGraphAPI:
    @staticmethod
    def async_run(payload):
        asyncio.run(DocumentToGraphAPI.run(payload))

    @staticmethod
    async def run(payload):
        documents = DocumentComposer().run(payload)

        async with asyncio.TaskGroup() as tg:
            for doc in documents:
                asyncio.create_task(DocumentToGraphAPI.doc_to_graph_runner(doc))

    @staticmethod
    async def doc_to_graph_runner(document) -> IS_COMPLETED:
        process = DocumentToNeo4J()
        process.update_or_create_document(document)
        process.update_or_create_relations(document)

        return True