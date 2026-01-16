///<reference path='../interfaces/IRecord.ts'/>
///<reference path='../interfaces/IDataChannel.ts'/>
///<reference path='../interfaces/IDataChannelUpdates.ts'/>

import _ = require('lodash');

export class TestDataChannel implements IDataChannel {

    private records: IRecord[];
    private updates: IUpdate[];
    private subscriptionId: number;
    private subscriptions: { [key: string] : IDataChannelUpdates };

    constructor(records: IRecord[]) {
        this.records = records;
        this.updates = [];
        this.subscriptionId = 1;
    }

    read(id:string): IRecord {
        return _.find(this.records, { id: id });
    }

    update(record: IRecord): IRecord {
        var toMerge = _.find(this.records, { id: record.id });

        if (toMerge === null) {
            return null;
        }

        _.merge(toMerge, record);

        return toMerge;
    }

    create(record: IRecord): void {
        if (_.any(this.records, { id: record.id})) {
            throw Error("Record already exists");
        }
        this.records.push(record);
    }

    getIds(): string[] {
        var ids = [];
        this.records.forEach((record: IRecord) => {
            ids.push(record.id);
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
        this.records.splice(index, 1);
    }

    subscribe(updates: IDataChannelUpdates): string {
        var subscriptionId = this.subscriptionId++;
        this.subscriptions[subscriptionId] = updates;
        return subscriptionId.toString();
    }

    unsubscribe(id: string): void {
        delete this.subscriptions[id];
    }

    getVersion(): number {
        return this.updates.length;
    }

    getUpdates(version:number): IUpdate[] {
        return this.updates.slice(version);
    }

    private onChange(type: number, id: string): void {
        var update: IUpdate = {
            type: type,
            id: id
        };

        _.each(this.subscriptions, (subscription: IDataChannelUpdates) => {
            subscription.onUpdate(update);
        });

        this.updates.push(update);
    }
}