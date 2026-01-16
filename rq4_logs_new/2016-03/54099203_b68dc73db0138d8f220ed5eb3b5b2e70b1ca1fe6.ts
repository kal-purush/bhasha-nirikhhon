import {Pipe} from "angular2/core";
import {ResourceService} from "../services/resource-service";
@Pipe({
    name: 'global'
})

export class GlobalSearchPipe {
    constructor(public resource:ResourceService) {
    }

    transform(dataArr, [filter]) {
        if (typeof dataArr === "undefined") {
            return;
        }
        if (typeof filter === 'undefined' || Object.keys(filter).length === 0 || filter === "") {
            return dataArr;
        }
        this.resource.data = [];
        dataArr.forEach((row) => {
            for (var value in row) {
                if (row.hasOwnProperty(value)) {
                    let element;
                    if (typeof row[value] === "object") {
                        //for now just ignore nested object
                        return;
                    }
                    if (typeof row[value] === "boolean") {
                        return;
                    }
                    if (typeof row[value] === "string") {
                        element = row[value].toLocaleLowerCase();
                    }
                    if (typeof row[value] === "number") {
                        element = row[value].toString();
                    }
                    if (element.indexOf(filter["value"].toLocaleLowerCase()) >= 0) {
                        this.resource.data.push(row);
                    }
                }
            }
        });

        return this.resource.data;
    }
}