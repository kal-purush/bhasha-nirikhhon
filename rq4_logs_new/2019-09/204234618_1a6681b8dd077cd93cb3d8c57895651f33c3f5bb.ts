import { Injectable } from '@angular/core';

import { Client } from './client';

@Injectable({
  providedIn: 'root'
})
export class ClientService {

  lastId = 0;

  clients: Client[];

  constructor() { }

  add(client: Client) {
    if (!client.id){
      client.id = ++this.lastId;
    };
    this.clients.push(client);
    return client;
  };

  update(client: Client) {

    const old = this.getById(client.id);
    if (!old){
      return;
    };

    Object.assign(old, client);
    return old;
  };

  delete(id: number) {
    this.clients = this.clients.filter(find => find.id !== id);
  };

  getById(id: number) {
    return this.clients.filter(find => find.id == id);
  };

  getAll(){
    return this.clients;
  };

}