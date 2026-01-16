'use strict';

export default function makeMap(objects: any, expectedKey: string){
    var myMap = new Map();

    Object.keys(objects).forEach((key) => {
        myMap.set(objects[key][expectedKey], objects[key]);
    });

    return myMap;
};