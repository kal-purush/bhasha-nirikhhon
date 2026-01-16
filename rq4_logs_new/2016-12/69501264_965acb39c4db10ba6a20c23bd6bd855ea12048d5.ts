'use strict';

import {MongoClient} from 'mongodb';
import {Configuration} from '../../support/configuration';

export class DatabaseClientService {

    private mongoClient = MongoClient;
    private connection;
    private mongodbURL: string;

    public init = () => {
        if (process.env.MONGO_DB_URL && process.env.MONGO_DB_URL !== undefined) {
            this.mongodbURL = process.env.MONGO_DB_URL;
        } else {
            this.mongodbURL = Configuration.mongoDbUrl;
        }
        console.log('connecting to Mongo Db database on :',this.mongodbURL);
    };

    private getConnection = () => {
        return new Promise((resolve, reject) => {
            if (this.connection === undefined) {
                this.mongoClient.connect(this.mongodbURL, (err, db) => {
                    if (err) {
                        console.log(err);
                        reject(err);
                    }
                    this.connection = db;
                    resolve(this.connection);
                });
            } else {
                resolve(this.connection);
            }
        });
    };

    // we should call this whenever application close
    private closeConnection = () => {
        this.connection.close();
        console.log('Connection to db closed');
    };


    // Insert into table
    // supports both, single record and bulk records
    public insert = (table, payload) => {
        return new Promise((resolve, reject) => {
            this.getConnection().then(db => {
                var collection = db.collection(table);

                if (Array.isArray(payload)) {
                    collection.insertMany(payload, (err, result) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(result);
                        }
                    });
                } else {
                    collection.insertOne(payload, (err, result) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(result);
                        }
                    });
                }
            }, err => {
                reject(err);
            });
        });
    };

    /** Update database **/
    public update = (table, searchFilter, updatedPayload) => {
        return new Promise((resolve, reject) => {
            this.getConnection().then(db => {
                var collection = db.collection(table);
                collection.updateOne(
                    searchFilter,
                    updatedPayload,
                    (err, result) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(result);
                        }
                    });

            }, err => {
                reject(err);
            });
        });
    };

    /** Find query to return data **/
    public find = (table, searchFilter) => {
        return new Promise((resolve, reject) => {
            this.getConnection().then(db => {
                var collection = db.collection(table);
                collection.find(searchFilter).toArray(function (err, docs) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(docs);
                    }

                });
            }, err => {
                reject(err);
            });
        });
    };

    // Drop table/collection
    public dropTable = table => {
        var collection = this.mongoClient.collection(table);
        return new Promise((resolve, reject) => {
            collection.drop((err, reply) => {
                if (err) {
                    reject(err);
                }
                resolve(reply);
            });
        });
    };

    //rename the collection/table
    public renameTable = (oldName, newName) => {
        var collection = this.mongoClient.collection(oldName);
        return Promise((resolve, reject) => {
            if (oldName === newName) {
                reject('old and new names shouldbe different');
            }

            collection.rename(newName, (err, newCollection) => {
                if (err) {
                    reject(err);
                }
                resolve(newCollection);
            });

        });

    };


    /** Exit function - Close connection before exit **/
    private exit = () => {
        this.closeConnection();
    };
}