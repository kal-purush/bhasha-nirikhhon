import {Inject, Injector, Injectable} from 'angular2/core';
import {Http, Response} from 'angular2/http';
import {Observable} from 'rxjs/Rx';

export class UrlProvider {
    
    private classParam : any
    private method : string
    private baseUrl : string
    
    constructor (classParam : any, method : string) {
        
        this.classParam = classParam
        this.method     = method
        this.baseUrl    = 'http://jsonplaceholder.typicode.com'
        
    }
    
    buildUrl () {
        
        switch (this.classParam.name)
            {
            case 'Post' :
                this.method === 'GET' ? this.baseUrl += '/posts' : this.baseUrl
                break
            case '' :
                break
                default : this.baseUrl
            } 
            
            return this.baseUrl
    }
    
}