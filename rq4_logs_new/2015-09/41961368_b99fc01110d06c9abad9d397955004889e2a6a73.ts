import { NedbDatabaseDataChannel } from '../dataChannels/NedbDatabaseDataChannel';
import { SynchronizedDictionary } from '../structure/SynchronizedDictionary';
import _ = require('lodash');

describe('Synchronized Dictionary', () => {
    it('should initialize contents based on existing data', (done) => {
        var dataChannel = new NedbDatabaseDataChannel();
        var dictionary = new SynchronizedDictionary(dataChannel, 'key');
        dictionary.setCacheUpdateInterval(0); // to update it for each request
        var record1 = <any> {
            id: '1',
            key: 'key1',
            value: 'value1'
        };
        var record2 = <any> {
            id: '2',
            key: 'key1',
            value: 'value2'
        };
        var record3 = <any> {
            id: '3',
            key: 'key2',
            value: 'value3'
        };
        dataChannel.create(record1, (error: Error) => {
            expect(error).toBeFalsy();
            dictionary.get('key1', (error: Error, records: IRecord[]) => {
                expect(error).toBeFalsy();
                expect(records.length).toBe(1);
                expect(records[0].id).toBe(record1.id);
                dataChannel.create(record2, (error: Error) => {
                    expect(error).toBeFalsy();
                    dictionary.get('key1', (error: Error, records: IRecord[]) => {
                        expect(error).toBeFalsy();
                        expect(records.length).toBe(2);
                        expect(_.find(records, { id: record1.id })).toBeTruthy();
                        expect(_.find(records, { id: record2.id })).toBeTruthy();
                        dataChannel.create(record3, (error: Error) => {
                            expect(error).toBeFalsy();
                            dictionary.get('key2', (error: Error, records: IRecord[]) => {
                                expect(error).toBeFalsy();
                                expect(records.length).toBe(1);
                                expect(records[0].id).toBe(record3.id);
                                dataChannel.remove(record2.id, (error: Error) => {
                                    expect(error).toBeFalsy();
                                    dictionary.get('key1', (error: Error, records: IRecord[]) => {
                                        expect(error).toBeFalsy();
                                        expect(records.length).toBe(1);
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
});