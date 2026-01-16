import { Component } from '@angular/core';
import { FirebaseService } from './firebase.service';
import { FormGroup }     from '@angular/forms';
 
@Component({
    moduleId: module.id,
    selector: 'firebase-form',
    templateUrl: 'firebase.component.html',
    styleUrls: [ 'firebase.component.css' ],
    providers: [FirebaseService]
})
 
export class FirebaseComponent {
    response: string;

    constructor(private _firebaseService: FirebaseService) {}

    onSubmit(form: FormGroup){
        this._firebaseService.setUser(form.value.firstName, form.value
        .lastName).subscribe(
            user => this.response = JSON.stringify(user),
            error => console.log(error)
        );
    }

    onGetUser(){
        this._firebaseService.getUser().subscribe(
            user => this.response = JSON.stringify(user),
            error => console.log(error)
        )
    }
}