'use strict';

require('zone.js');

function zone(callback: () => void) {
    return function(done: () => Promise<void>) {
        Zone.current.fork({
            name: 'test',
            onHandleError: (parentZoneDelegate: ZoneDelegate, currentZone: Zone, targetZone: Zone, error: any) => {
                fail(error.rejection);
                done();
                return false;
            }
        }).run(async () => {
            await callback();
            done();
        });
    };

};

export function asyncBeforeEach(callback: () => Promise<void>) {
    beforeEach(zone(callback));
}

export function asyncIt(desc: string, callback: () => Promise<void>) {
    it(desc, zone(callback));
}