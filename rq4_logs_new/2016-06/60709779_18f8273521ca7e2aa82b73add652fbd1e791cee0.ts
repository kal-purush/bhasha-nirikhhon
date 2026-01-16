import { Container, Application, ApplicationOptions, KernelInterface } from 'bap-node-microframework/core';
import * as express from "express";
import { MongoosePlugin } from 'bap-node-microframework-mongoose';
import { CorsPlugin } from 'bap-node-microframework-cors';

declare function require(path: string, options?: any): any;

Container.setParameter('appDirectory', __dirname);
Container.registerService('router', express.Router());

Container.setParameter('odm', true);

import { Kernel } from "./kernel";
var kernel = new Kernel();
var App = new Application(<ApplicationOptions>{
    sockets: true,
    oauth: false
}, <KernelInterface>kernel);

App.registerPlugin(MongoosePlugin, { "dsn": "mongodb://db:27017" });
App.registerPlugin(CorsPlugin, {});

App.start();

module.exports = {
    server: App.httpServer,
    Container: Container
};