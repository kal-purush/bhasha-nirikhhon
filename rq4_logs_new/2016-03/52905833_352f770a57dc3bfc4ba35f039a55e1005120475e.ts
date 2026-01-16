import {Schema, TextInput, NumberInput, FormsConfig} from 'marvelous-aurelia-forms';

export class Configuration {
  schema: Schema;
  submitted;  
  formModel: { firstName?: string, lastName?: string, age?: number, amount?: number } = {};

  constructor() {    
    let form = {
      firstName: new TextInput({ label: 'First Name', validators: { required: true } }),
      lastName: new TextInput({ label: 'Last Name', validators: { required: true } }),
      age: new NumberInput({ label: 'Age', type: 'integer' }),
      amount: new NumberInput({ label: 'Amount', type: 'decimal' })
    }
    
    this.schema = new Schema(form);
    
    // overrides default configuration
    this.schema.config = {
      validation: {
        // by default validation triggers on blur only when form has been submitted at least once
        // this configuration will result with form being validated on every blur 
        shouldValidateOnBlur: () => true
      }
    }
  }

  submit() {
    this.submitted = JSON.stringify(this.formModel, null, 2);
  }
}