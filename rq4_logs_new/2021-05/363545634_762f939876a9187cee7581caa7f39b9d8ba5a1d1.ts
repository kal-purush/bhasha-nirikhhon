import axios, {AxiosInstance} from "axios";

export interface Device {
    name: string;
    macAddress: string;
    latitude: number;
    preference: string;
    timezone: string;
    roomType: string;
    deviceType: string;
    longitude: number;
    spaceType: string;
    deviceUUID: string;
    deviceId: number;
    locationName: string;
}

export interface Usage {
    scope: string;
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

export interface SensorReading {
    comp: string;
    value: number;
}

export interface SensorIndex {
    comp: string;
    value: number;
}

export interface AirData {
    timestamp: string;
    score: number;
    sensors: SensorReading[];
    indices: SensorIndex[];
}

export class Awair {
    axiosInstance: AxiosInstance;
    deviceType: string | null;
    deviceId: number | null;

    constructor(bearerToken: string) {
        this.axiosInstance = axios.create({
            baseURL: "https://developer-apis.awair.is/v1/",
            headers: {
                Authorization: `Bearer ${bearerToken}`,
            },
        });
        this.deviceType = null;
        this.deviceId = null;
    }

    setDevice(deviceType: string, deviceId: number): void {
        this.deviceType = deviceType;
        this.deviceId = deviceId;
    }

    clearDevice(): void {
        this.deviceType = null;
        this.deviceId = null;
    }

    async getDevices(): Promise<Device[]> {
        const response = await this.axiosInstance.get("users/self/devices");
        return response.data.devices;
    }

    async getUser(): Promise<User> {
        const response = await this.axiosInstance.get("users/self");
        return response.data;
    }

    async getDeviceAPIUsage(
        deviceType?: string,
        deviceId?: number
    ): Promise<Usage[]> {
        const response = await this.axiosInstance.get(
            `users/self/devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/api-usages `
        );
        return response.data.usages;
    }

    async getLatestAirData(
        deviceType?: string,
        deviceId?: number
    ): Promise<AirData | null> {
        const response = await this.axiosInstance.get(
            `users/self/devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/air-data/latest`
        );
        return response.data.data?.[0] ?? null;
    }

    async getRawAirData(
        deviceType?: string,
        deviceId?: number
    ): Promise<AirData[]> {
        const response = await this.axiosInstance.get(
            `users/self/devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/air-data/raw`
        );
        return response.data;
    }

    async getDeviceDisplayMode(
        deviceType?: string,
        deviceId?: number
    ): Promise<string> {
        const response = await this.axiosInstance.get(
            `devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/display`
        );
        return response.data.mode;
    }

    async getDeviceKnockingMode(
        deviceType?: string,
        deviceId?: number
    ): Promise<string> {
        const response = await this.axiosInstance.get(
            `devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/knocking`
        );
        return response.data.mode;
    }

    async getDeviceLEDMode(
        deviceType?: string,
        deviceId?: number
    ): Promise<{mode: string; brightness?: number}> {
        const response = await this.axiosInstance.get(
            `devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/led`
        );
        return response.data;
    }

    async getDevicePowerStatus(
        deviceType?: string,
        deviceId?: number
    ): Promise<{percentage: number; plugged: boolean; timestamp: string}> {
        const response = await this.axiosInstance.get(
            `devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/power-status`
        );
        return response.data;
    }

    async getDeviceTimeZone(
        deviceType?: string,
        deviceId?: number
    ): Promise<string> {
        const response = await this.axiosInstance.get(
            `devices/${deviceType ?? this.deviceType}/${
                deviceId ?? this.deviceId
            }/timezone`
        );
        return response.data.timezone;
    }
}