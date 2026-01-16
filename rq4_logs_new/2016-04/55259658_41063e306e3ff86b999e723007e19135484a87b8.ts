/// <reference path="../../Scripts/typings/tsd.d.ts" />

import express = require('express');

export class Routes {
        
        musicians: string = './controllers/musicians';
        musiciansController: MusiciansController;
        
        constructor() {
            this.musiciansController = new MusiciansController();
        }
        
        GetRoutes(app) {
            app.get('/musicians', (request, response) => { this.musiciansController.GetAll(request, response) } );
            //app.get('/musicians/:id', this.musiciansController.Get(1));
            //app.post('/musicians', this.musiciansController.Add());
            //app.put('/musicians/:id', this.musiciansController.Update());
            //app.delete('/musicians/:id', this.musiciansController.Delete(1));
        }
        
    }

export class MusiciansController {
        
    //routes: Routes
        
    constructor() {
        //this.routes = new Routes();
    }
        
    GetAll(request: express.Request, response: express.Response) {
            
        console.log('Request... ', request);
        console.log('Response', response);
            
        response.send([{
            'id': 1,
            'name': 'Max',
            'band': 'Maximum Pain',
            'instrument': 'guitar'
        }]);   
    }
        
    Get(id: number) {
            
    }
        
    Add() {
            
    }
        
    Update() {
            
    }
        
    Delete(id: number) {
            
    }
}