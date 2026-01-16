from pydantic import BaseModel

from pydantic_settings import BaseSettings


class HarvestCff(BaseModel):
    enable_validation: bool = False


class HarvestSettings(BaseModel):
    from_: list[str] = [ "cff", "git" ]
    cff: HarvestCff = HarvestCff()


class DepositTargetSettings(BaseModel):
    site_url: str = 'https://sandbox.zenodo.org'
    communities: list[str] = ["zenodo"]
    access_right: str = "open"


class DepositSettings(BaseModel):
    target: str = 'invenio_rdm'
    invenio_rdm: DepositTargetSettings = DepositTargetSettings()


class HermesSettings(BaseModel):
    harvest: HarvestSettings = HarvestSettings()
    deposit: DepositSettings = DepositSettings()


class Settings(BaseSettings):
    hermes: HermesSettings = HermesSettings()