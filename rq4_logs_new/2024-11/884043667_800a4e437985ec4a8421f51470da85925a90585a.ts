import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';
import { iBankInfo } from '../../models/iBankInfo';
import { MapsService } from '../../service/maps.service';
import Swal from 'sweetalert2';

@Component({
  selector: 'point-info',
  templateUrl: './point-info.component.html',
  styleUrl: './point-info.component.css',
})
export class PointInfoComponent implements OnChanges {
  @Input() flag: boolean = false;
  @Input() id_blood_bank: number = 0;

  place: iBankInfo = {
    name_place: '',
    phone_number: '',
    address: {
      street: '',
      distrit: '',
      locality: '',
      postal_code: '',
      state: '',
    },
  };

  constructor(private _serviceMaps: MapsService) {}
  
  ngOnChanges(changes: SimpleChanges): void {
    this.loadInfo();
  }

  loadInfo(): void {
    if (this.flag) {
      this._serviceMaps.getDetailisPlace(this.id_blood_bank).subscribe({
        next: (response) => {
          this.place = response;
        },
        error: (err) => {
          Swal.fire({
            position: 'top',
            icon: 'error',
            title: 'Ha ocurrido un error',
            showConfirmButton: false,
            timer: 1500,
          });
          console.log(err);
        },
      });
    }
  }
}