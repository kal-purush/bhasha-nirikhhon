//import {computedFrom} from 'aurelia-framework';
import {Validation,ValidationGroup} from 'aurelia-validation';

export class Welcome {
  static inject() { return [Validation]; }
  //
  public heading:string;
  public firstName:string;
  public lastName:string;
  private previousValue:string;
  public validation:ValidationGroup;
  //
  constructor(validation)  {
    this.heading = 'Welcome to the Aurelia Navigation App!';
    this.firstName = 'John';
    this.lastName = 'Doe';
    this.previousValue = this.fullName;
    this.validation = validation.on(this)
        .ensure('firstName')
              .isNotEmpty()
              .hasMinLength(3)
              .hasMaxLength(10)
        .ensure('lastName')
              .isNotEmpty()
              .hasMinLength(3)
              .hasMaxLength(10) ;
  }

  //Getters can't be directly observed, so they must be dirty checked.
  //However, if you tell Aurelia the dependencies, it no longer needs to dirty check the property.
  //To optimize by declaring the properties that this getter is computed from, uncomment the line below
  //as well as the corresponding import above.
  //@computedFrom('firstName', 'lastName')
  get fullName() {
    return `${this.firstName} ${this.lastName}`;
  }
/*
  submit() {
    this.previousValue = this.fullName;
    alert(`Welcome, ${this.fullName}!`);
  }
  */
  submit() {
    this.validation.validate() //the validate will fulfil when validation is valid, and reject if not
      .then( () => {
        alert(`Welcome, ${this.fullName}! `);
      }).catch((e)=>{
				
			});
  }
  canDeactivate() {
    if (this.fullName !== this.previousValue) {
      return confirm('Are you sure you want to leave?');
    }
  }
}

export class UpperValueConverter {
  toView(value) {
    return value && value.toUpperCase();
  }
}