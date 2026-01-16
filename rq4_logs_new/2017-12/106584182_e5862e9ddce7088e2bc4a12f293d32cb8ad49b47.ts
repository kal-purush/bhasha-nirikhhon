import { DataModelInstance, GeoDataClass } from '../models/UDX-data.model';
import * as Promise from 'bluebird';
const debug = require('debug');
const initDebug = debug('WebNJGIS: Init');
import * as mongoose from 'mongoose';

const initData = (data: GeoDataClass): Promise<any> => {
    return new Promise((resolve, reject) => {
        DataModelInstance.find(data._id)
            .then(docs => {
                if (docs.length >= 1) {
                    initDebug('Init data succeed!' + data._id);
                    return resolve();
                } else {
                    DataModelInstance.insert(data)
                        .then(rst => {
                            initDebug('Init data succeed!' + data._id);
                            return resolve();
                        })
                        .catch(err => {
                            initDebug(err);
                            return reject(err);
                        });
                }
            })
            .catch(err => {
                initDebug(err);
                return reject(err);
            });
    });
};


// const datas = [
//     {
//         _id: new mongoose.Types.ObjectId(),
//         gdid: '',
//         filename: '',
//         path: '',
//         permission: 'public',
//         userId: 'Admin'
//     }
// ];

// Promise.all()