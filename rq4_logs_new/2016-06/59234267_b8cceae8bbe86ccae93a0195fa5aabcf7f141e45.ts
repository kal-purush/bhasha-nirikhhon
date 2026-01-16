import { StoreService } from './store.service';
import { DatastoreService } from './datastore/datastore.service';
import { ConnectionService } from './connection/connection.service';
import { DeviceService } from './device/device.service';
import { AccessService } from './access/access.service';
import { HTTP_PROVIDERS } from '@angular/http';
export * from './store.service';
export * from './access/access.service';
export * from './connection/connection.service';
export * from './datastore/datastore.service';
export * from './device/device.service';

/**
 * Created by tom on 21.06.16.
 */
export const VALO_PLUS_PROVIDERS: any[] = [
    StoreService, DatastoreService, ConnectionService, DeviceService, AccessService, HTTP_PROVIDERS
];