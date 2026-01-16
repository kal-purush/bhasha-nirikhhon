import {
    Device,
    DeviceLEDMode,
    DevicePowerStatus,
    DeviceAPIUsage,
    User,
    AirData,
} from "./types";

export const MOCK_DEVICE: Device = {
    name: "Mock Device",
    macAddress: "0123456789AB",
    latitude: 43.6542052,
    preference: "PRODUCTIVITY",
    timezone: "America/New_York",
    roomType: "BEDROOM",
    deviceType: "awair-element",
    longitude: -70.2500918,
    spaceType: "HOME",
    deviceUUID: "awair-element_1234",
    deviceId: 1234,
    locationName: "Portland",
};

export const MOCK_POWER_STATUS: DevicePowerStatus = {
    percentage: 91,
    plugged: true,
    timestamp: "2019-05-16T22:21:04.160Z",
};

export const MOCK_LED_MODE: {mode: DeviceLEDMode; brightness: number} = {
    mode: "MANUAL",
    brightness: 100,
};

export const MOCK_USER: User = {
    usages: [
        {scope: "USER_DEVICE_LIST", usage: 1},
        {scope: "USER_INFO", usage: 1},
    ],
    tier: "Hobbyist",
    email: "jones@example.com",
    permissions: [
        {scope: "FIFTEEN_MIN", quota: 100},
        {scope: "FIVE_MIN", quota: 300},
        {scope: "RAW", quota: 500},
        {scope: "LATEST", quota: 300},
        {scope: "PUT_PREFERENCE", quota: 300},
        {scope: "PUT_DISPLAY_MODE", quota: 300},
        {scope: "PUT_LED_MODE", quota: 300},
        {scope: "PUT_KNOCKING_MODE", quota: 300},
        {scope: "PUT_TIMEZONE", quota: 300},
        {scope: "PUT_DEVICE_NAME", quota: 300},
        {scope: "PUT_LOCATION", quota: 300},
        {scope: "PUT_ROOM_TYPE", quota: 300},
        {scope: "PUT_SPACE_TYPE", quota: 300},
        {scope: "GET_DISPLAY_MODE", quota: 300},
        {scope: "GET_LED_MODE", quota: 300},
        {scope: "USER_DEVICE_LIST", quota: 2147483647},
        {scope: "USER_INFO", quota: 2147483647},
        {scope: "GET_KNOCKING_MODE", quota: 300},
        {scope: "GET_POWER_STATUS", quota: 300},
        {scope: "GET_TIMEZONE", quota: 300},
    ],
    sex: "UNKNOWN",
    lastName: "Jones",
    firstName: "Doris",
    id: "8888",
};

export const MOCK_DEVICE_API_USAGES: DeviceAPIUsage[] = [
    {scope: "FIVE_MIN", usage: 4},
    {scope: "GET_DISPLAY_MODE", usage: 2},
    {scope: "GET_KNOCKING_MODE", usage: 1},
    {scope: "GET_LED_MODE", usage: 2},
    {scope: "GET_POWER_STATUS", usage: 1},
    {scope: "GET_TIMEZONE", usage: 1},
    {scope: "LATEST", usage: 3},
    {scope: "PUT_DISPLAY_MODE", usage: 1},
    {scope: "PUT_LED_MODE", usage: 5},
    {scope: "RAW", usage: 8},
];

export const MOCK_AIR_DATA: AirData[] = [
    {
        timestamp: "2021-05-16T22:21:25.000Z",
        score: 89,
        sensors: [
            {comp: "pm25", value: 3},
            {comp: "voc", value: 165},
            {comp: "co2", value: 458},
            {comp: "humid", value: 32.720001220703125},
            {comp: "temp", value: 25.43000030517578},
        ],
        indices: [
            {comp: "voc", value: 0},
            {comp: "co2", value: 0},
            {comp: "pm25", value: 0},
            {comp: "temp", value: 0},
            {comp: "humid", value: -2},
        ],
    },
    {
        timestamp: "2021-05-16T22:21:15.000Z",
        score: 89,
        sensors: [
            {comp: "pm25", value: 2},
            {comp: "voc", value: 172},
            {comp: "co2", value: 458},
            {comp: "humid", value: 32.709999084472656},
            {comp: "temp", value: 25.40999984741211},
        ],
        indices: [
            {comp: "voc", value: 0},
            {comp: "co2", value: 0},
            {comp: "pm25", value: 0},
            {comp: "temp", value: 0},
            {comp: "humid", value: -2},
        ],
    },
    {
        timestamp: "2021-05-16T22:21:05.000Z",
        score: 89,
        sensors: [
            {comp: "pm25", value: 2},
            {comp: "voc", value: 167},
            {comp: "co2", value: 457},
            {comp: "humid", value: 32.599998474121094},
            {comp: "temp", value: 25.40999984741211},
        ],
        indices: [
            {comp: "voc", value: 0},
            {comp: "co2", value: 0},
            {comp: "pm25", value: 0},
            {comp: "temp", value: 0},
            {comp: "humid", value: -2},
        ],
    },
];