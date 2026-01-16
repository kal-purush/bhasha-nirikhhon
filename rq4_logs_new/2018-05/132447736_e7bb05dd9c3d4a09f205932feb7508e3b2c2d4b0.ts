import { Injectable } from '@angular/core';

@Injectable()
export class PersonalInformationService {

  private personalInformation: any = null;

  constructor() {}

  setPersonalInformation(personalInformation: any) {
    this.personalInformation = personalInformation;
  }

  getPersonalInformation() {
    return this.personalInformation;
  }

}