import { Vehicle } from './../../shared/vehicle';
import { ModalController } from '@ionic/angular';
import { Component, OnInit, Input, Output } from '@angular/core';
import { User } from 'src/app/shared/user';

@Component({
  selector: 'app-modal',
  templateUrl: './modal.page.html',
  styleUrls: ['./modal.page.scss'],
})
export class ModalPage implements OnInit {

   @Input() name: string;
   @Input() holder: User;
   @Input() load: number;
   @Input() maxLoad: number;
   @Input() volume: number;
   @Input() seats: number;
   @Input() maxSeats: number;

  constructor(public modal: ModalController) { }

  ngOnInit() {
  }

  saveModal(){
    const vehicle: Vehicle = new Vehicle(this.name, this.holder, this.load, this.maxLoad, this.volume, this.seats, this.maxSeats);

    //this.modal.dismiss(vehicle);
    console.log( 'saved! ' + this.name + ' ' + this.load);
  }

  dismissModal(){
    this.modal.dismiss();
  }

}