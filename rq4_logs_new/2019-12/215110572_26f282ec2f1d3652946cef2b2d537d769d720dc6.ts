import {Injectable} from '@angular/core';
import {Observable, of} from "rxjs";
import {Restaurant} from "../_models/restaurant";
import {Lobby} from "../_models/lobby";

@Injectable({
    providedIn: 'root'
})
export class ApiService {

    constructor() { }

    getRestaurants(): Observable<Restaurant[]> {
        return of([
            {
                id: 1,
                name: 'McDonald\'s',
                address: 'Kromera, Wrocław',
                menu: [
                    'Burger',
                    'Frytki',
                    'Cola',
                ]
            },
            {
                id: 2,
                name: 'Burger King',
                address: 'Kromera, Wrocław',
                menu: [
                    'Burger',
                    'Frytki',
                    'Cola',
                ]
            },
            {
                id: 3,
                name: 'KFC',
                address: 'Kromera, Wrocław',
                menu: [
                    'Burger',
                    'Frytki',
                    'Cola',
                ]
            },
        ]);
    }

    getLobbies(): Observable<Lobby[]> {
        return of([
            {
                ownerId: 123,
                expirationDate: new Date('2019-12-24'),
                location: { lat: 51.1264, lon: 16.9918 },
                restaurant: {
                    id: 3,
                    name: 'KFC',
                    address: 'Kromera, Wrocław',
                    menu: [
                        'Burger',
                        'Frytki',
                        'Cola',
                    ]
                },
                lobbyAddress: 'Lobby address',
            },
            {
                ownerId: 123,
                expirationDate: new Date('2019-12-24'),
                location: { lat: 51.1292, lon: 16.9708 },
                restaurant: {
                    id: 2,
                    name: 'Burger King',
                    address: 'Plac Dominikański, Wrocław',
                    menu: [
                        'Burger',
                        'Frytki',
                        'Cola',
                    ]
                },
                lobbyAddress: 'Nokia West link',
            },
        ]);
    }
}