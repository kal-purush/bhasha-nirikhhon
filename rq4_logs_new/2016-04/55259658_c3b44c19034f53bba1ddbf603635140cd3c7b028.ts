/// <reference path="../../Scripts/typings/tsd.d.ts" />

import express = require('express');

export interface User {
    firstName: string;
    lastName: string;
    department: string;
}

// musicians.controller.js
export class MusiciansController {
    
    private user: User = null;;
    
    constructor() {
        this.user = {firstName: 'brock', lastName: 'billings', department: 'IT App Dev'}
        
        // TODO: Need to set Base URL...
    }
    
    /*
     * Gets all the musicians.
     */
    GetAll(request: express.Request, response: express.Response) {
        //request.authenticatedUser = this.user;
        
        //console.log('Authenticated User... ', request.authenticatedUser, '\n');
        
        console.log('The local address ', request.connection.localAddress, ' was accessed on port ', request.connection.localPort, '...\n');
        console.log('Response Status Code... ', response.statusCode, '\n');
        
        response
        .status(response.statusCode)
        .json([{
            'id': 1,
            'name': 'Max',
            'band': 'Maximum Pain',
            'instrument': 'guitar'
        },
        {
            'id': 2,
            'name': 'Jax',
            'band': 'Maximum Pain',
            'instrument': 'drums'
        }]);
    }
        
    Get(request: express.Request, response: express.Response, id: number) {
        response
        .status(response.statusCode)
        .json([{
            'id': 1,
            'name': 'Max',
            'band': 'Maximum Pain',
            'instrument': 'guitar'
        }]);
    }
        
    Add() {
            
    }
        
    Update() {
            
    }
        
    Delete(id: number) {
            
    }
}