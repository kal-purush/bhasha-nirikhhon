import { Injectable } from '@angular/core';
import * as _ from 'lodash';
@Injectable()
export class CommonService {
    constructor() { }
    finalGenerateReport(csvFullData, headarraylist) {
        let totalHeadsReport = {};
        _.forEach(headarraylist, (value, key) => {
            totalHeadsReport[value['head']] = 0
        })
        _.forEach(csvFullData, (csv, key) => {
            const head = _.find(headarraylist, { '_id': csv['Assigned_head_id'] });
            totalHeadsReport[head['head']] += csv['Transaction_Amount(INR)']
        })
        if (totalHeadsReport['Income'] && totalHeadsReport['Transfer']) {
            totalHeadsReport['Income-Transfer'] = totalHeadsReport['Income'] - totalHeadsReport['Transfer']
        }
        const finalData = [];
        _.forEach(totalHeadsReport, (value, key) => {
            finalData.push({
                'name': key,
                'value': value
            })
        })
        return finalData;
    }
}