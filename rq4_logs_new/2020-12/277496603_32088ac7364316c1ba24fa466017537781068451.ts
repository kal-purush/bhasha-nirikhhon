/* eslint-disable @typescript-eslint/no-unused-vars */
import { ValidatorConstraint, ValidatorConstraintInterface, ValidationArguments } from 'class-validator';
import * as moment from 'moment';

@ValidatorConstraint({ name: 'notLessThanEighteen', async: false })
export class NotLessThanEighteen implements ValidatorConstraintInterface {
  validate(date: string, args: ValidationArguments) {
    const eighteenYearsAgo = moment().subtract(18, 'years');
    const birthday = moment(date);
    return eighteenYearsAgo.isAfter(birthday);
  }

  defaultMessage(args: ValidationArguments) {
    // here you can provide default error message if validation failed
    return 'You must be 18+ to sign up!';
  }
}