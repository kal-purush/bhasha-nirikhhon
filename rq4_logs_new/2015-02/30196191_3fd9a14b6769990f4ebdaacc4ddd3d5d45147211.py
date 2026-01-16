from suds.xsd.doctor import Import
from suds.xsd.doctor import ImportDoctor
from suds.client import Client


url = 'https://se-face-webservice.redsara.es/sspp?wsdl'
tns = 'https://webservice.face.gob.es'
imp = Import('http://schemas.xmlsoap.org/soap/encoding/', 'http://schemas.xmlsoap.org/soap/encoding/')
imp.filter.add(tns)

client = Client(url,plugins=[ImportDoctor(imp)])
# print client

result = client.service.consultarAdministraciones()
print result