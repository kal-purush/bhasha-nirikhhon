import {AxiosRequestConfig} from "axios";

export type DeviceRoomType =
    | "LIVING_ROOM"
    | "NURSERY"
    | "BEDROOM"
    | "KITCHEN"
    | "OFFICE"
    | "OTHERS"
    | "BASEMENT"
    | "DINING_ROOM"
    | "HALLWAY"
    | "BATHROOM"
    | "PATIO"
    | "GARAGE"
    | "ATTIC"
    | "MEETING_ROOM"
    | "RESTROOM"
    | "BOARD_ROOM"
    | "EXECUTIVE_SUITE"
    | "LOUNGE"
    | "LOBBY"
    | "LABORATORY";

export type DeviceSpaceType = "HOME" | "OFFICE" | "OTHERS";

export type DevicePreference =
    | "GENERAL"
    | "PRODUCTIVITY"
    | "SLEEP"
    | "ALLERGY"
    | "BABY";

export interface Device {
    name: string;
    macAddress: string;
    latitude: number;
    preference: DevicePreference;
    timezone: string;
    roomType: DeviceRoomType;
    deviceType: string;
    longitude: number;
    spaceType: DeviceSpaceType;
    deviceUUID: string;
    deviceId: number;
    locationName: string;
}

export type UsageScope = "FITEEN_MIN" | "FIVE_MIN" | "LATEST" | "RAW";

export interface Usage {
    scope: UsageScope;
    usage: number;
}

export interface Permission {
    scope: string;
    usage: number;
}

export interface User {
    usages: Usage[];
    tier: string;
    email: string;
    dobYear?: number;
    permissions: Permission[];
    sex: string;
    lastName: string;
    firstName: string;
    id: string;
}

export type SensorIndexComp =
    | "temp"
    | "humid"
    | "co2"
    | "voc"
    | "pm25"
    | "dust";

export type SensorReadingComp = SensorIndexComp | "lux" | "spl_a";

export interface SensorReading {
    comp: SensorReadingComp;
    value: number;
}

export interface SensorIndex {
    comp: SensorIndexComp;
    value: number;
}

export interface AirData {
    timestamp: string;
    score: number;
    sensors: SensorReading[];
    indices: SensorIndex[];
}

export type DeviceDisplayMode =
    | "clock"
    | "co2"
    | "default"
    | "humid"
    | "nightlight"
    | "off"
    | "pm25"
    | "score"
    | "status"
    | "temp"
    | "temp_humid_celsius"
    | "temp_humid_fahrenheit"
    | "voc";

export type DeviceKnockingMode = "on" | "off" | "sleep";

export type DeviceClockMode = "12hr" | "24hr";

export type DeviceBrightness = 0 | 20 | 40 | 60 | 80 | 100;

export type DeviceTemperatureUnit = "c" | "f";

export type DeviceLEDMode = "auto" | "dim" | "manual" | "on" | "sleep";

export interface Options {
    deviceType?: string;
    deviceId?: number;
    axiosConfig?: AxiosRequestConfig;
    bearerToken?: string;
}

export interface SetDeviceDisplayModeOptions extends Options {
    mode: DeviceDisplayMode;
    clockMode?: DeviceClockMode;
    brightness?: DeviceBrightness;
    temperatureUnit?: DeviceTemperatureUnit;
}

export interface SetDeviceKnockingModeOptions extends Options {
    mode: DeviceKnockingMode;
}

export interface SetDeviceLEDModeOptions extends Options {
    mode: DeviceLEDMode;
    /* This one can be any integer between 0 and 100, so we're not using
       DeviceBrightness. */
    brightness?: number;
}

export interface SetDeviceLocationOptions extends Options {
    latitude: number;
    longitude: number;
}

export interface SetDeviceNameOptions extends Options {
    name: string;
}

export interface SetDevicePreferenceOptions extends Options {
    preference: DevicePreference;
}

export interface SetDeviceRoomTypeOptions extends Options {
    roomType: DeviceRoomType;
}

export interface SetDeviceSpaceTypeOptions extends Options {
    spaceType: DeviceSpaceType;
}