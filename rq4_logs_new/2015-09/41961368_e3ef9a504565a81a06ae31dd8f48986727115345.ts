///<reference path='../interfaces/IRecord.ts'/>
///<reference path='../interfaces/IDataChannel.ts'/>
///<reference path='../interfaces/IDataChannelUpdates.ts'/>

import _ = require('lodash');
import { Constants } from '../interfaces/Constants';

class Subscription {
    callback: IDataChannelUpdates;
    filter: any;
}

export class MemoryDataChannel implements IDataChannel {

    private records: IRecord[];
    private recordMap: { [key: string] : IRecord };
    private updates: IUpdate[];
    private subscriptionId: number;
    private subscriptions: { [key: string] : Subscription };

    constructor(records?: IRecord[]) {
        this.records = records || [];
        this.updates = [];
        this.subscriptionId = 1;
        this.subscriptions = {};
        this.recordMap = {};
        _.each(this.records, (record: IRecord) => {
            this.recordMap[record.id] = record;
        });
    }

    read(id:string): IRecord {
        return this.recordMap[id];
    }

    update(record: IRecord): IRecord {
        var toMerge = _.find(this.records, { id: record.id });

        if (toMerge === null) {
            return null;
        }

        _.merge(toMerge, record);
        this.onChange(Constants.UPDATE_CHANGED, record.id, this.recordMap[record.id]);

        return toMerge;
    }

    create(record: IRecord): void {
        if (this.recordMap.hasOwnProperty(record.id)) {
            throw Error("Record already exists");
        }
        this.records.push(record);
        this.recordMap[record.id] = record;
        this.onChange(Constants.UPDATE_CREATED, record.id, record);
    }

    private static filterField(field: any, test: any) {
        if (_.isString(test)) {
            if (!_.isString(field)) {
                return false;
            }
            var expression = new RegExp(test);
            if (!expression.test(field)) {
                return false;
            }
        } else if (_.isNumber(test)) {
            if (!_.isNumber(field)) {
                return false;
            }
            if (test !== field) {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    private static filterRecord(record: IRecord, filter?: any): boolean {
        if (!filter) {
            return true;
        }
        if (_.isObject(filter)) {
            for (var key in filter) {
                if (!filter.hasOwnProperty(key)) {
                    continue;
                }
                var test = filter[key];
                var field = record[key];
                if (!MemoryDataChannel.filterField(field, test)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    getIds(filter?: any): string[] {
        var ids = [];
        this.records.forEach((record: IRecord) => {
            if (MemoryDataChannel.filterRecord(record, filter)) {
                ids.push(record.id);
            }
        });
        return ids;
    }

    getAllRecords(): IRecord[] {
        return this.records;
    }

    remove(id: string): void {
        var index = _.findIndex(this.records, { id: id });
        if (index < 0) {
            throw Error("Cannot find record");
        }
        var record = this.recordMap[id];
        this.records.splice(index, 1);
        delete this.recordMap[id];
        this.onChange(Constants.UPDATE_DELETED, id, record);
    }

    subscribe(updates: IDataChannelUpdates, filter?: any): string {
        var subscriptionId = this.subscriptionId++;
        this.subscriptions[subscriptionId.toString()] = {
            callback: updates,
            filter: filter
        };
        return subscriptionId.toString();
    }

    unsubscribe(id: string): void {
        delete this.subscriptions[id];
    }

    getVersion(): number {
        return this.updates.length;
    }

    getUpdates(version:number, filter?: any): IUpdate[] {
        return _
            .drop(this.updates, version)
            .filter((update: IUpdate) => MemoryDataChannel.filterRecord(this.recordMap[update.id], filter));
    }

    private onChange(type: number, id: string, record: IRecord): void {

        var update: IUpdate = {
            type: type,
            id: id
        };

        _.each(this.subscriptions, (subscription: Subscription) => {
            if (MemoryDataChannel.filterRecord(record, subscription.filter)) {
                subscription.callback.onUpdate(update);
            }
        });

        this.updates.push(update);
    }
}