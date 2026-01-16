import { Injectable } from '@angular/core';
import { StorageService } from './storage.service';

@Injectable({providedIn: 'root'})
export class SessionStorageService implements StorageService {

    getItem<T>(key: string): Promise<T> {
        const value = JSON.parse(sessionStorage.getItem(key));
        return Promise.resolve(value);
    }

    setItem<T>(key: string, value: any): Promise<void> {
        const result = sessionStorage.setItem(key , JSON.stringify(value));
        return Promise.resolve(result);
    }

    getItemSync<T>(key: string) {
        const value = JSON.parse(sessionStorage.getItem(key));
        return value;
    }
    setItemSync<T>(key: string, value: any){
        const result = sessionStorage.setItem(key , JSON.stringify(value));
        return result;
    }

    removeItem(key: string): Promise<void> { 
        return Promise.resolve(sessionStorage.removeItem(key));
    }
}