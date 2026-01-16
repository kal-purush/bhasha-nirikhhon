from sqlalchemy.orm import Session
from station.app.crud.crud_fhir_servers import fhir_servers
from fhir_kindling import FhirServer
from station.app.config import Settings


def fhir_server_from_db(db: Session, fhir_server_id: int) -> FhirServer:
    db_server = fhir_servers.get(db, id=fhir_server_id)

    assert db_server
    # for each of the different options access and decrypt the sensitive values and initialize a server instance
    if db_server.username:
        password = Settings.get_fernet().decrypt(db_server.password.encode()).decode()
        return FhirServer(
            api_address=db_server.api_address,
            username=db_server.username,
            password=password,
            fhir_server_type=db_server.type
        )

    if db_server.token:
        token = Settings.get_fernet().decrypt(db_server.token.encode()).decode()
        return FhirServer(
            api_address=db_server.api_address,
            token=token,
            fhir_server_type=db_server.type
        )

    if db_server.client_id:
        client_secret = Settings.get_fernet().decrypt(db_server.client_secret.encode())
        print(client_secret)
        print(client_secret.decode("utf-8"))
        return FhirServer(
            api_address=db_server.api_address,
            fhir_server_type=db_server.type,
            client_id=db_server.client_id,
            client_secret=client_secret.decode("utf-8"),
            oidc_provider_url=db_server.oidc_provider_url,
        )