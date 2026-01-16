/**
 * Service to access information about the device running on.
 *
 * Created by tom on 18.06.16.
 */
import { Injectable } from '@angular/core';
import { Device } from 'ionic-native/dist/index';

@Injectable()
export class DeviceService implements DeviceService {
    public getDeviceId():String {
        let uuid = Device.device.uuid;
        if (uuid === undefined) {
            uuid = 'devuuid';
        }
        return uuid;
    }
}

export interface DeviceService {
    /**
     * Returns the id of the Device
     */
    getDeviceId():String;
}