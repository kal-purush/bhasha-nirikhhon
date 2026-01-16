import { Injectable, Output, EventEmitter } from '@angular/core';
import { Router } from '@angular/router';
import { HttpClient } from '@angular/common/http';
import { Location, PlatformLocation } from '@angular/common';

const baseUrl = location.origin + "/api/";

@Injectable({
    providedIn: 'root'
})


export class dataAccessService {
    
    constructor(private http:HttpClient, ) {}    
  
    getAllChips(){        
        return this.http.get(baseUrl + "Chips/getChips");
    }

    
    
}

interface ApiResponse{
    success: boolean,
    secret:string
}