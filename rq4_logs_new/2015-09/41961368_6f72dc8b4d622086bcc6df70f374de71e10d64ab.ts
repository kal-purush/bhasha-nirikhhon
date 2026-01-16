///<reference path='../interfaces/IRecord.ts'/>
///<reference path='../interfaces/IDataChannel.ts'/>
///<reference path='../interfaces/IDataChannelUpdates.ts'/>

import Datastore = require('nedb');
import _ = require('lodash');
import { Constants } from '../interfaces/Constants';

export class NedbDatabaseDataChannel implements IDataChannel {

    private db = new Datastore();
    private versionDb = new Datastore();
    private versionId: number = 0;

    constructor() {
    }

    private getVersionId(): number {
        return (Date.now()*1000) + ((this.versionId++) % 1000);
    }

    private getQuery(filter: any): any {
        var query: any = {};
        if (!_.isObject(filter)) {
            return query;
        }
        _.forOwn(filter, (test, field) => {
            if (_.isString(test)) {
                query[field] = new RegExp(test);
            } else if (_.isNumber(test)) {
                query[field] = test;
            }
        });
        return query;
    }

    getIds(filter: any, callback: (error: Error, ids?: string[]) => void) {
        this.db.find(this.getQuery(filter), {
            _id: 1
        }, (err, docs) => {
            if (err) {
                callback(err);
                return;
            }
            var ids = [];
            _.each(docs, (doc: any) => {
                ids.push(doc._id);
            });
            callback(err, ids);
        });
    }

    read(id: string, callback: (error: Error, record?: IRecord) => void) {
        this.db.findOne({
            _id: id
        }, (error: Error, doc: any) => {
            if (error) {
                callback(error);
                return;
            }
            var record = _.cloneDeep(doc);
            record.id = id;
            delete record._id;
            callback(error, record);
        });
    }

    update(record: IRecord, callback: (error: Error, record?: IRecord) => void) {
        var newRecord:any = _.cloneDeep(record);
        var id = newRecord.id;
        delete newRecord.id;
        newRecord._id = id;
        this.db.update({ _id: id}, newRecord, (err: Error, updated: number) => {
            if (err) {
                callback(err);
                return;
            }
            if (updated === 0) {
                callback(new Error("No documents to update"));
                return;
            }
            newRecord.id = id;
            delete newRecord._id;
            this.versionDb.insert({
                created: this.getVersionId(),
                id: id,
                type: Constants.UPDATE_CHANGED
            }, (err: Error) => {
                if (err) {
                    callback(err);
                    return;
                }
                callback(err, newRecord);
            });
        });
    }

    create(record: IRecord, callback: (error: Error, record?: IRecord) => void) {
        var newRecord:any = _.cloneDeep(record);
        if (newRecord.hasOwnProperty("id")) {
            newRecord._id = newRecord.id;
            delete newRecord.id;
        }
        this.db.insert(newRecord, (err: Error, doc: any) => {
            if (err) {
                callback(err);
                return;
            }
            newRecord = _.cloneDeep(doc);
            newRecord.id = newRecord._id;
            delete newRecord._id;
            this.versionDb.insert({
                created: this.getVersionId(),
                id: newRecord.id,
                type: Constants.UPDATE_CREATED
            }, (err: Error) => {
                if (err) {
                    callback(err);
                    return;
                }
                callback(err, newRecord);
            });
        });
    }

    remove(id: string, callback: (error: Error) => void) {
        this.db.remove({
            _id: id
        }, {}, (error: Error) => {
            if (error) {
                callback(error);
                return;
            }
            this.versionDb.insert({
                created: this.getVersionId(),
                id: id,
                type: Constants.UPDATE_DELETED
            }, callback);
        })
    }

    getVersion(callback: (error: Error, version?: string) => void) {
        this.versionDb.find({}).sort({created: -1}).limit(1).exec((error, docs?: any[]) => {
            if (error) {
                callback(error);
                return;
            }
            callback(error, docs.length ? docs[0].created.toString() : null);
        });
    }

    getUpdates(from: string, filter: any, callback: (error: Error, updates?: IUpdate[]) => void) {
        var query: any = {};
        if (from) {
            query.created = {
                $gt: parseInt(from)
            }
        }
        this.versionDb.find(query).sort({
            created: 1
        }).exec((error, docs?: any[]) => {
            if (error) {
                callback(error);
                return;
            }

            function prepareUpdates(docs: any[]) {
                var updates: IUpdate[] = [];
                _.each(docs, (doc: any) => {
                    var update = {
                        type: doc.type,
                        id: doc.id,
                        version: doc.created.toString()
                    };
                    updates.push(update);
                });
                callback(error, updates);
            }

            if (filter) {
                var query = this.getQuery(filter);
                query._id = {
                    $in: _.map(docs, (doc: any) => doc.id)
                };
                this.db.find(query, { _id: 1 }, (error: Error, ids: any[]) => {
                    var idsMap = {};
                    _.each(ids, (id) => {
                        idsMap[id._id] = true;
                    });
                    docs = _.filter(docs, (doc) => {
                        return idsMap.hasOwnProperty(doc.id);
                    });
                    prepareUpdates(docs);
                });
            } else {
                prepareUpdates(docs);
            }
        });
    }
}