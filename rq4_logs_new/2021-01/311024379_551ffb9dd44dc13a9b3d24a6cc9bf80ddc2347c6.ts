import assert from 'assert';
import { SearchConsistency } from 'ottoman';
import { hawk } from './setup/fixtures';
import { getOttomanModel } from './setup/model';
import { removeDocuments } from './setup/util';

describe('test replaceById function', async () => {
    it('ottoman - should replace doc', async () => {
        const Airplane = getOttomanModel();
        const hawkAirplane = new Airplane(hawk);
        const created = await Airplane.create(hawkAirplane);
        const findbefore = await Airplane.find({},
            {
                consistency: SearchConsistency.LOCAL
            });
        assert.strictEqual(findbefore.rows.length, 1);
        assert.strictEqual(findbefore.rows[0].callsign, 'Hawk');

        const change = await Airplane.replace({
            callsign: 'abc',
            name: 'ABC Airlines',
            model: 'A380',
            location: {
                type: 'Point',
                coordinates: [
                    1.22,
                    2.33,
                    1.11
                ]
            },
            },
            created.id);
        const find = await Airplane.find(
            {},
            {
                consistency: SearchConsistency.LOCAL
            });
        assert.strictEqual(find.rows[0].callsign, change.callsign);

        await removeDocuments();
    });

    it('ottoman - should not replace doc as it is a partial and incomplete doc', async () => {
        const Airplane = getOttomanModel();
        const hawkAirplane = new Airplane(hawk);
        const created = await Airplane.create(hawkAirplane);
        const findbefore = await Airplane.find({},
            {
                consistency: SearchConsistency.LOCAL
            });
        assert.strictEqual(findbefore.rows.length, 1);
        assert.strictEqual(findbefore.rows[0].callsign, 'Hawk');

        const change = await Airplane.replace({
            callsign: 'abc',
            },
            created.id);
        const find = await Airplane.find(
            {},
            {
                consistency: SearchConsistency.LOCAL
            });
        assert.strictEqual(find.rows[0].callsign, change.callsign);

        await removeDocuments();
    });
})