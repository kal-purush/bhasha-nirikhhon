import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { DonorsService } from '../../service/donors.service';
import { iParams } from '../../models/iParams';
import { iDonorSearch } from '../../models/iDonorSearch';


@Component({
  selector: 'filters',
  templateUrl: './filters.component.html',
  styleUrl: './filters.component.css',
})
export class FiltersComponent implements OnInit {
  @Output() setDonors = new EventEmitter<iDonorSearch[]>();

  bloodTypes: string[] = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'];
  localities: string[] = [];

  params: iParams = {
    bloodType: '',
    locality: '',
    compatibility: '',
  };

  constructor(private _donorsService: DonorsService) {}

  ngOnInit(): void {
    this._donorsService.getLocalities().subscribe({
      next: (response) => {
        response.forEach((locaclity) => {
          this.localities.push(locaclity.locality);
        });
      },
      error: (err) => {
        console.error(err);
      },
    });
  }

  initSearch(): void {
    console.log(this.params);
    this.caseSearch();
    this.resetSearch();
  }

  isBloodTypeDisabled(): boolean {
    // Deshabilita el selector de tipo de sangre si se ha seleccionado compatibilidad
    return this.params.compatibility !== '';
  }

  isCompatibilityDisabled(): boolean {
    // Deshabilita el selector de compatibilidad si se ha seleccionado tipo de sangre
    return this.params.bloodType !== '';
  }

  resetSearch(): void {
    this.params.bloodType = '';
    this.params.compatibility = '';
    this.params.locality = '';
  }

  caseSearch(): void {
    const { bloodType, locality, compatibility } = this.params;

    if (bloodType != '' && locality == '' && compatibility == '') {
      console.log('Solo tipo de sangre');
      this._donorsService.getByBloodType(bloodType).subscribe({
        next: (response) => {
          this.setDonors.emit(response);
        },
        error: (err) => {
          if (err.status == 404) {
            return console.log('No hubo resultados');
          }
          console.log(err);
        },
      });
    }
    if (bloodType == '' && locality != '' && compatibility == '') {
      console.log('Solo tipo de localidad');
      this._donorsService.getByLocality(locality).subscribe({
        next: (response) => {
          this.setDonors.emit(response);
        },
        error: (err) => {
          if (err.status == 404) {
            return console.log('No hubo resultados');
          }
          console.log(err);
        },
      });
    }
    if (bloodType == '' && locality == '' && compatibility != '') {
      console.log('Solo tipo de compatibilidad');
      this._donorsService.getByCompatibility(compatibility).subscribe({
        next: (response) => {
          this.setDonors.emit(response);
        },
        error: (err) => {
          if (err.status == 404) {
            return console.log('No hubo resultados');
          }
          console.log(err);
        },
      });
    }
    if (bloodType != '' && locality != '' && compatibility == '') {
      console.log('Solo tipo de sangre y localidad');
      this._donorsService
        .getByBloodTypeLocality(bloodType, locality)
        .subscribe({
          next: (response) => {
            this.setDonors.emit(response);
          },
          error: (err) => {
            if (err.status == 404) {
              return console.log('No hubo resultados');
            }
            console.log(err);
          },
        });
    }
    if (bloodType == '' && locality != '' && compatibility != '') {
      console.log('Solo localidad y compaitbilidad');
      this._donorsService
        .getByCompatibilityLocality(compatibility, locality)
        .subscribe({
          next: (response) => {
            this.setDonors.emit(response);
          },
          error: (err) => {
            if (err.status == 404) {
              return console.log('No hubo resultados');
            }
            console.log(err);
          },
        });
    }
  }
}