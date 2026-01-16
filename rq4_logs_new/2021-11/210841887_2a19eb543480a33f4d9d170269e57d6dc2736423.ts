import { Injectable } from '@angular/core';
import { Facility, Group, Resource, Service, User, Vo } from '@perun-web-apps/perun/openapi';

@Injectable({
  providedIn: 'root'
})

/* Service to store immutable entity information such as ids and bean name. */
export class EntityStorageService {

   entity: Vo | Group | Facility | Resource | Service | User;

   setEntity(entity: Vo | Group | Facility | Resource | Service | User) {
     this.entity = entity;
   }

  getEntity(): Vo | Group | Facility | Resource | Service | User {
     return this.entity;
   }

}