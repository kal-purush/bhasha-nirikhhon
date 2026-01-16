from d2d.contracts.payload import DocumentService
from d2d.tasks.document_composer import DocumentComposer
from d2d.tasks.payload_handler import convert_structural_payload

from .documents_dispatcher import DocumentServicesDispatcher


class DocumentsEndpoint:
    """This endpoint converts payload into document, then execute services"""

    # def async_run(self, payload):
    #     asyncio.run(DocumentToGraphAPI.run(payload))

    @classmethod
    def run(cls, payload):
        payload_obj = convert_structural_payload(payload)

        if not payload_obj.document_services:
            err = "document_services cannot be empty when using DocumentEndpoint"
            raise AttributeError(err)

        # convert payload to documents
        documents = DocumentComposer().run(payload_obj)

        # run on all the services
        for document in documents:
            for service in payload_obj.document_services:
                _ = cls.run_service(document, service)

        return True

    @classmethod
    def run_service(cls, document, model: DocumentService):
        return DocumentServicesDispatcher.service_runner(
            document=document,
            plugin_name=model.plugin_name,
            service_name=model.service_name,
        )