//2514208539 : Sandeep (Ineraction Id)////

import {Injectable} from '@angular/core'
import {Resolve} from '@angular/router'
import {EventService} from './shared/event.service'
import {ActivatedRouteSnapshot} from '@angular/router'

@Injectable()
export class EventResolverService implements Resolve<any>{

    constructor (private eventService: EventService){

    }
    //Resolve method also accept current activated route 
    resolve(route :ActivatedRouteSnapshot){ // if we are calling using resolver then we not need to subscribe as resovler does same for us
        console.log(route.params['id'], ' : id');
        return this.eventService.getEventById(route.params['id']).map(event => event)
    }
} 