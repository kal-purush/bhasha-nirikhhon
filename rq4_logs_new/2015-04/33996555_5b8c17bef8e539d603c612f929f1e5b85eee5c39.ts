import express = require('express');
var parse:any = require('csv-parse');
var moment:any = require('moment');
import fsManager = require('../../fsManager/main');
import Res = require('../ResForm');

var router = express.Router();
router

    .get('/', (req, res, next)=> {
        fsManager.list((err, data)=> {
            res.json(new Res(err, data));
        });
    })

    .get('/:logName', (req, res, next)=> {
        var logName = req.params.logName;

        fsManager.getLogFile(logName, (err, log:string)=> {
            // Parse CSV string
            parse(log, function (err, records:any[][]) {

                // Get node and sensor information from header row
                var nodeNames = records.shift();
                var sensors = records.shift();

                // Remove header column
                nodeNames.shift();
                sensors.shift();

                var data = {};
                /**
                 * Process records
                 */
                    // Collect nodes & sensors
                records.forEach((record, index, arr)=> {
                    var dateStr = record.shift();
                    var date:Date = moment(dateStr, 'YYMMDD_HHmmss');
                    //dates.push(date);

                    record.filter((value, index, arr)=> {
                        if (value == '')
                            return false;

                        var nodeName = nodeNames[index];
                        var sensorName = sensors[index];

                        if (data[nodeName] == undefined)
                            data[nodeName] = {};
                        if (!Array.isArray(data[nodeName][sensorName]))
                            data[nodeName][sensorName] = [];

                        data[nodeName][sensorName].push(
                            {
                                value: Number(value),
                                //date: dates[index]
                                date: date
                            });
                    });
                });
                // Change data structure: Object -> Array
                var nodes = [];
                // Node processing
                for (var nodeName in data) {
                    var node = {
                        name: nodeName,
                        sensors: []
                    };
                    // Sensor processing
                    for (var sensorName in data[node.name]) {
                        var log = data[node.name][sensorName];
                        var sensor = {
                            name: sensorName,
                            log: log
                        };
                        // Filter log
                        sensor.log = sensor.log.filter((record, index, arr)=> {
                            if (index == 0)
                                return true;

                            return record['value'] != arr[index - 1].value;
                        });
                        // Push sensor
                        node.sensors.push(sensor);
                    }
                    // Push node
                    nodes.push(node);
                }
                // Set date string
                nodes.forEach((node)=> {
                    node.sensors.forEach((sensor:any)=> {
                        sensor.log.forEach((record:any)=> {
                            record.date = new Date(record.date).toLocaleTimeString();
                        })
                    });
                });

                // Response
                res.json(new Res(err, nodes));
            });
        });
    });

export = router;