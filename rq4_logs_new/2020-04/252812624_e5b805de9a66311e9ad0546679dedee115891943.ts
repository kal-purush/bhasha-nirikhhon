import { Injectable } from '@angular/core';
import { AndroidPermissions } from '@ionic-native/android-permissions/ngx';

@Injectable()
export class AndroidPermissionService {
    constructor (private androidPermissions: AndroidPermissions) {
    }

    requestNecessaryPermissions () {
        let androidPermissionsList = [
            this.androidPermissions.PERMISSION.CAMERA,
            this.androidPermissions.PERMISSION.WRITE_EXTERNAL_STORAGE
        ];
        return this.androidPermissions.requestPermissions(androidPermissionsList);
    }
}