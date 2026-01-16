/// <reference path="./typings/main" />
import * as PiStation from '../client/PiStation.ts';
import {PiStationServer, PiStationServerEvent} from './app/server';

const app = new PiStationServer();

app.addModule()

app.on(`${PiStation.Events.GetAllModules}`).subscribe(function (event : PiStationServerEvent) {
    console.log('Asking all modules!', event);
    var mockModules = [
        new PiStation.Module(
            'kakuLights',
            [
                new PiStation.Function('powerControl', [new PiStation.Argument('enabled', 'bool')]),
                new PiStation.Function('dim', [new PiStation.Argument('dimmingLevel', 'bit')])
            ]
        )
    ];
    console.log('Returning:', mockModules);
    event.socket.emit(`${PiStation.Events.GetAllModules}`, mockModules);
});

app.on(`${PiStation.Events.ClientDisconnected}`).subscribe(function (event : PiStationServerEvent) {
    console.log("disconnecting");
});