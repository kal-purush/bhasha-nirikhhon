import { loadData } from '../../tests/utils.test';
import { Attribute, Data } from '../data/Data';
import DataFilterer from '../data/DataFilterer';
import { testDataFilename2 as filename } from '../../tests/utils.test';
import TableWidget from './TableWidget';
import SetElement from '../data/setutils/SetElement';

describe('TableWidget.tsx', () => {
    let data: Data, filterer: DataFilterer;
    beforeEach(async () => {
        data = await loadData(filename);
        filterer = new DataFilterer(data);
    });
    describe('processAsArray can group rows as expected', () => {
        it('Group by Probability', () => {
            const results = TableWidget.processAsArray(
                filterer.getData()[0],
                filterer.getData()[1],
                [data.getIndexForColumn(Attribute.probability)],
                false
            );
            expect(results[0][1]).toBe(1);
            expect(results[1][1]).toBe(1);
            expect(results[0][0]).not.toBe(results[1][0]);
        });
        it('Group by Species Group', () => {
            const results = TableWidget.processAsArray(
                filterer.getData()[0],
                filterer.getData()[1],
                [data.getIndexForColumn(Attribute.speciesGroup)],
                false
            );
            expect(results.length).toBe(1); //there is only 1 species group here
        });
        it('Group by Scientific Name', () => {
            const results = TableWidget.processAsArray(
                filterer.getData()[0],
                filterer.getData()[1],
                [data.getIndexForColumn(Attribute.speciesLatinName)],
                false
            );
            expect(
                (
                    filterer.getData()[0][results[0][0]][
                        data.getIndexForColumn(Attribute.speciesLatinName)
                    ] as SetElement
                ).value
            ).toBe('Cygnus olor');
        });
        it('Culling empty rows works', () => {
            const results = TableWidget.processAsArray(
                filterer.getData()[0],
                filterer.getData()[1],
                [
                    data.getIndexForColumn(Attribute.callType),
                    data.getIndexForColumn(Attribute.speciesLatinName)
                ],
                true
            );
            expect(results.length).toBe(0);
        });
    });
});