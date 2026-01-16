'use strict';
import * as restify from "restify";
import {RequestHandler} from "restify";
import {SRBEvent} from "lib_srbevent";

export class Router {
  constructor(private server: restify.Server, private port: number) {
  }

  addRoute(method: string, route: string, endpoint: RequestHandler) {
    method = method.toUpperCase();
    switch (method) {
      case 'DELETE':
      case 'DEL':
        this.server.del(route, endpoint);
        break;
      case 'HEAD':
        this.server.head(route, endpoint);
        break;
      case 'PUT':
        this.server.put(route, endpoint);
        break;
      case 'GET:':
      default:
        this.server.get(route, endpoint);
        break;
    }
  }

  start() {
    this.server.listen(this.port);
    SRBEvent.event.emit('router_listening', this.port);
  }
}