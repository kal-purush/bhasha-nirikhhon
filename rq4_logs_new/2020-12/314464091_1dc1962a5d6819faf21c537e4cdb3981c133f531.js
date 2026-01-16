import { extend } from 'vee-validate';
import { 
  required,
  email,
  min,
  confirmed,
  alpha_dash} from 'vee-validate/dist/rules';

extend('required', {
  ...required,
  message: 'The {_field_} field is required'
});


extend('email', {
  ...email,
  message: 'Enter avalid email !'
});

extend('min', {
  ...min,
  message: 'The {_field_} field must have at least {length} characters'
});

extend('confirmed', {
  ...confirmed,
  message: 'The {_field_} field must match field above!'
});

extend('alpha_dash', {
  ...alpha_dash,
  message: 'The {_field_} field may contain alpha-numeric characters as well as dashes and underscores'
});