from pydantic import BaseModel
from config.config import DIRECTUS_CREATE_RESOURCE_ENDPOINT, DIRECTUS_TOKEN
from core.output import RESTAdapter
from core.types import AntevortaDiscovery

directus_adapter = RESTAdapter(
    endpoint=DIRECTUS_CREATE_RESOURCE_ENDPOINT,
    token=DIRECTUS_TOKEN,
    method="POST",
)


class WebhookAccountability(BaseModel):
    user: str
    role: str


class DirectusWebhookEvent(BaseModel):
    event: str
    accountability: WebhookAccountability
    payload:  AntevortaDiscovery
    key: str
    collection: str