import grpc
from identifier_pb2_grpc import IdentifiersStub
from identifier_pb2 import IdentifierRequest
from identifier_pb2 import IdentifierOption


channel = grpc.insecure_channel("localhost:50051")
client = IdentifiersStub(channel)
request = IdentifierRequest(identifier_option=IdentifierOption.CUSA)

print(client.GetIdentifiers(request))