import { Response, Request, NextFunction } from 'express';
import * as formidable from 'formidable';
import * as Promise from 'bluebird';
import * as _ from 'lodash';
import * as path from 'path';
import * as fs from 'fs';
import { calcuTaskDB, CalcuTaskState } from '../models';


export const startMS = (nodeName: string, token?: string): Promise<any> => {
    return new Promise((resolve, reject) => {
        let tasks;
        calcuTaskDB.find({
            nodeName: nodeName,
            state: CalcuTaskState.INIT
        })
            .then(docs => {
                tasks = docs;
                return Promise.resolve();
            })
            .then(() => {
                return calcuTaskDB.update({
                    nodeName: nodeName,
                    state: CalcuTaskState.INIT
                }, {
                    '$set': {
                        state: CalcuTaskState.START_PENDING
                    }
                }, {
                    multi: true
                })
            })
            .then(updateRst => {
                if(updateRst.ok && updateRst.writeErrors.length === 0) {
                    return resolve(tasks);
                }
                else {
                    return reject(new Error('error in update calculate tasks!'));
                }
            })
            .catch(reject);
    });
};

export const updateState = (nodeName: string, taskId: string, oldState: number, newState: number): Promise<any> => {
    return new Promise((resolve, reject) => {
        if(oldState === CalcuTaskState.START_PENDING) {
            if(newState !== CalcuTaskState.START_FAILED
            && newState !== CalcuTaskState.RUNNING) {
                return reject(new Error('invalidate state change!'));
            }
        }
        else if(oldState === CalcuTaskState.RUNNING) {
            if(newState !== CalcuTaskState.RUN_FAILED
            && newState !== CalcuTaskState.RUN_SUCCEED) {
                return reject(new Error('invalidate state change!'));
            }
            if(newState === CalcuTaskState.RUN_SUCCEED) {
                // TODO 更新cmp-task
            }
        }
        else if(oldState === CalcuTaskState.PAUSE) {
            // TODO
        }

        calcuTaskDB.update({
            _id: taskId,
            nodeName: nodeName
        }, {
            '$set': {
                state: newState
            }
        })
            .then(rst => {
                if(rst.ok && rst.writeErrors.length === 0) {
                    return resolve();
                }
                else {
                    return reject(new Error('update calculate task state failed!'));
                }
            })
            .catch(reject);
    })
};