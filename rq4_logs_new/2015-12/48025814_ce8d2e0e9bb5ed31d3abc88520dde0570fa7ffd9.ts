///<reference path="../../typings/tsd.d.ts"/>

import iListItem = require('../interfaces/iListItem');
import iChartInfo = require("../interfaces/iChartInfo");

export = Sorting;

class Sorting {
    public transformMapToArray(map:any):iListItem[] {
        var array:iListItem[] = _.map(map, (value:any, key:string) => {
            return <iListItem>{key:key, value:value};
        });

        array = _.sortBy(array, (item:iListItem) => {
            return item.value;
        });

        return array.reverse();
    }

    public getChartInfo(map:any, series:string):iChartInfo {
        var array = this.transformMapToArray(map);
        var labels = [];
        var data = [];
        _.each(array, (item:iListItem) => {
            labels.push(item.key);
            data.push(item.value);
        });
        return <iChartInfo>{
            series: [series],
            labels: labels,
            data: [data]
        };
    }

    public mapKeyCount(map:any, key:string) {
        if(map[key]) {
            map[key]++;
        }
        else {
            map[key] = 1;
        }
    }
}