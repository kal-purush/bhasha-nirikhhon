/**
 * Service to access domain Objects. This service should help to get the local storage synced with the Database and the controller.
 * Created by tom on 19.06.16.
 */

import { AccessService } from './access.service';
import { beforeEachProviders, expect, inject, it, describe } from '@angular/core/testing';

describe('Access Service Test', () => {
    beforeEachProviders(() => [AccessService]);

    it('Testing get UUID', inject([AccessService], (accessService:AccessService) => {
        expect(accessService.saveChannelInDevice(null, null)).toEqual('devuuid');
    }));
});