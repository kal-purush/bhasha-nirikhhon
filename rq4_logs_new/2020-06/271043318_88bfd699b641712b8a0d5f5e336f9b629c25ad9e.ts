import {Injectable} from '@angular/core';
import {AngularFirestore, AngularFirestoreCollection} from '@angular/fire/firestore';
import {Vehicle} from './vehicle';
import {Shipment} from './shipment';
import {User} from './user';
import {UserService} from './user.service';
import {AuthService} from './auth.service';
import {Person} from './person';
import {Observable} from 'rxjs';
import {map} from 'rxjs/operators';

@Injectable({
    providedIn: 'root'
})
export class ShipmentService {
    vehicleCollection: AngularFirestoreCollection<Vehicle>;
    userCollection: AngularFirestoreCollection<User>;
    shipmentCollection: AngularFirestoreCollection<Shipment>;

    static prepare(shipment: Shipment): Shipment {
        const copy = {...shipment};
        delete copy.id;

        copy.start = copy.start || null;
        copy.goal = copy.goal || null;
        copy.date = copy.date || null;
        copy.length = copy.length || null;
        copy.height = copy.height || null;
        copy.weight = copy.weight || null;
        copy.pricePerKg = copy.pricePerKg || null;
        copy.seat = copy.seat || null;
        copy.cargonaut = copy.cargonaut || null;
        copy.pricePerSeat = copy.pricePerSeat || null;
        copy.vehicle = copy.vehicle || null;
        copy.articleList = copy.articleList || null;
        copy.passengerList = copy.passengerList || null;
        copy.startTime = copy.startTime || null;

        return copy;
    }

    constructor(private afs: AngularFirestore, private userService: UserService, private authService: AuthService) {
        this.shipmentCollection = afs.collection<Shipment>('shipment');
    }

    // create shipment
    persist(id: string, cargonaut: string, vehicle: Vehicle, passengerList: Person[], articleList: [], start: string, goal: string, date: Date, startTime: string, length: number, height: number, weight: number,
            pricePerKg: number, seat: number, pricePerSeat: number) {
        const shipment: Shipment = new Shipment(id, cargonaut, vehicle, passengerList, articleList, start, goal, date, startTime, length, height, weight, pricePerKg, seat, pricePerSeat);
        return this.shipmentCollection.doc(shipment.id).set(ShipmentService.prepare(shipment));
    }

    update(id: string, cargonaut: string, vehicle: Vehicle, passengerList: Person[], articleList: [], start: string, goal: string, date: Date, startTime: string, length: number, height: number, weight: number,
           pricePerKg: number, seat: number, pricePerSeat: number) {
        const shipment: Shipment = new Shipment(id, cargonaut, vehicle, passengerList, articleList, start, goal, date, startTime, length, height, weight, pricePerKg, seat, pricePerSeat);
        return this.shipmentCollection.doc(shipment.id).update(ShipmentService.prepare(shipment));
    }

    delete(shipmentId: string) {
        this.shipmentCollection.doc(shipmentId).delete();
    }

    getShipment(id: string): Observable<Shipment> {
        return this.shipmentCollection.doc(id).get().pipe(
            map(a => {
                const data = a.data();
                data.id = a.id;
                return {...data} as Shipment;
            })
        );
    }

    query(value: string): Observable<Shipment[]>{
        const queryBase = this.afs.collection<Shipment>('shipment', ref => ref.orderBy(value)).snapshotChanges();
        return queryBase.pipe(
            map(actions => actions.map(a => {
                const data = a.payload.doc.data() as Shipment;
                data.id = a.payload.doc.id;
                return {...data};
            }))
        );
    }

}