import {TestBed, inject} from '@angular/core/testing';

import {GainService} from './gain.service';

describe('GainService', () => {
    beforeEach(() => {
        TestBed.configureTestingModule({
            providers: [GainService]
        });
    });

    it('should be created', inject([GainService], (service: GainService) => {
        expect(service).toBeTruthy();
    }));
});