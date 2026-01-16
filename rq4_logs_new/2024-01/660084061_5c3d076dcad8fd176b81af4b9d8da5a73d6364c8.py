"""
SpaceAPI v13 

https://github.com/SpaceApi/schema
"""

from typing import List, Optional

from pydantic import BaseModel, HttpUrl
from pydantic_extra_types.coordinate import Latitude, Longitude

from .sensors import (
    SpaceAPIv13BarometerSensorModel,
    SpaceAPIv13BaseSensorModel,
    SpaceAPIv13BeverageSupplySensorModel,
    SpaceAPIv13HumiditySensorModel,
    SpaceAPIv13TemperatureSensorModel,
    SpaceAPIv13NetworkConnectionSensorModel,
    SpaceAPIv13PeoplePresentSensorModel,
    SpaceAPIv13PowerConsumptionSensorModel,
    SpaceAPIv13TotalMemberCountSensorModel,
    SpaceAPIv13WindSensorModel,
    SpaceAPIv13AccountBalanceSensorModel,
    SpaceAPIv13RadiationSensorModel,
)


class SpaceAPIv13LocationModel(BaseModel):
    address: Optional[str] = None
    lat: Latitude
    lon: Longitude
    timezone: Optional[str] = None


class SpaceAPIv13SpacefedModel(BaseModel):
    spacenet: bool = False
    spacesaml: bool = False
    spacephone: bool = False


class SpaceAPIv13KeymastersModel(BaseModel):
    name: Optional[str] = None
    irc_nick: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    twitter: Optional[str] = None


class SpaceAPIv13ContactModel(BaseModel):
    phone: Optional[str] = None
    sip: Optional[str] = None
    keymasters: Optional[List[SpaceAPIv13KeymastersModel]] = None
    irc: Optional[str] = None
    twitter: Optional[str] = None
    identica: Optional[str] = None
    foursquare: Optional[str] = None
    email: Optional[str] = None
    ml: Optional[str] = None
    jabber: Optional[str] = None
    issue_mail: Optional[str] = None
    gopher: Optional[str] = None


class SpaceAPIv13StateIconModel(BaseModel):
    open: HttpUrl
    closed: HttpUrl


class SpaceAPIv13StateModel(BaseModel):
    open: bool = False
    lastchange: Optional[int] = None
    trigger_persion: Optional[str] = None
    message: Optional[str] = None
    icon: Optional[SpaceAPIv13StateIconModel] = None


class SpaceAPIv13EventModel(BaseModel):
    name: str
    type: str
    timestamp: int
    extra: str


class SpaceAPIv13SensorsModel(BaseModel):
    temperature: Optional[List[SpaceAPIv13TemperatureSensorModel]] = None
    door_locked: Optional[List[SpaceAPIv13BaseSensorModel]] = None
    barometer: Optional[List[SpaceAPIv13BarometerSensorModel]] = None
    radiation: Optional[List[SpaceAPIv13RadiationSensorModel]] = None
    humidity: Optional[List[SpaceAPIv13HumiditySensorModel]] = None
    beverage_supply: Optional[List[SpaceAPIv13BeverageSupplySensorModel]] = None
    power_consumption: Optional[List[SpaceAPIv13PowerConsumptionSensorModel]] = None
    wind: Optional[List[SpaceAPIv13WindSensorModel]] = None
    network_connections: Optional[List[SpaceAPIv13NetworkConnectionSensorModel]] = None
    account_balance: Optional[List[SpaceAPIv13AccountBalanceSensorModel]] = None
    total_member_count: Optional[List[SpaceAPIv13TotalMemberCountSensorModel]] = None
    people_now_present: Optional[List[SpaceAPIv13PeoplePresentSensorModel]] = None


class SpaceAPIv13FeedModel(BaseModel):
    type: Optional[str] = None
    url: HttpUrl


class SpaceAPIv13FeedsModel(BaseModel):
    blog: Optional[SpaceAPIv13FeedModel] = None
    wiki: Optional[SpaceAPIv13FeedModel] = None
    calendar: Optional[SpaceAPIv13FeedModel] = None
    flickr: Optional[SpaceAPIv13FeedModel] = None


class SpaceAPIv13Model(BaseModel):
    api: str = "0.13"
    space: str
    logo: HttpUrl
    url: HttpUrl
    location: SpaceAPIv13LocationModel
    spacefed: Optional[SpaceAPIv13SpacefedModel] = None
    cam: Optional[List[HttpUrl]] = None
    state: Optional[SpaceAPIv13StateModel] = None
    events: Optional[List[SpaceAPIv13EventModel]] = None
    contact: SpaceAPIv13ContactModel
    issue_report_channels: Optional[List[str]] = None
    sensors: Optional[SpaceAPIv13SensorsModel] = None
    feeds: Optional[SpaceAPIv13FeedsModel] = None
    projects: Optional[List[str]] = None