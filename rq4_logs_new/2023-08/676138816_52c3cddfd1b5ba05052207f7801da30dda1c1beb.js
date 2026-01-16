import * as Yup from 'yup';

export const stepOneValidation = Yup.object().shape({
  email: Yup.string().email('Invalid email').required('Email is required'),
  password: Yup.string().required('Password is required'),
  first_name: Yup.string().required('First name is required'),
  last_name: Yup.string().required('Last name is required'),
  phone: Yup.string().required('Zip Code is required'),
  password_confirmation: Yup.string()
    .oneOf([Yup.ref('password'), null], 'Passwords must match')
    .required('Confirm Password is required'),
});

export const StepOneInitials = {
  email: '',
  first_name: '',
  last_name: '',
  password: '',
  password_confirmation: '',
  phone: '',
};
export const StepOneFields = [
  {
    title: "Personal Information",
    fields: [
      { name: 'email', label: 'Email', type: 'email' },
      { name: 'first_name', label: 'First Name', type: 'text' },
      { name: 'last_name', label: 'Last Name', type: 'text' },
      { name: 'phone', label: 'Phone', type: 'number' },
      { name: 'password', label: 'Password', type: 'password' },
      { name: 'password_confirmation', label: 'Confirm Password', type: 'password' }
    ]
  },
];

export const stepTwoValidation = Yup.object().shape({
  country: Yup.string().required('Country is required'),
  state: Yup.string().required('State is required'),
  city: Yup.string().required('City is required'),
  address: Yup.string().required('Address is required'),
  zipcode: Yup.string().required('Zip Code is required'),
});

export const StepTwoInitials = {
  country: '',
  state: '',
  city: '',
  address: '',
  zipcode: '',
};
export const StepTwoFields = [
  {
    title: "Address Information",
    fields: [
      { name: 'address', label: 'Address', type: 'text' },
      { name: 'country', label: 'Country', type: 'text' },
      { name: 'state', label: 'State', type: 'text' },
      { name: 'city', label: 'City', type: 'text' },
      { name: 'zipcode', label: 'Zip Code', type: 'number' }
    ]
  },
];


export const stepThreeValidation = Yup.object().shape({
  account_number: Yup.string().required('Account Number is required'),
  business_type: Yup.string().required('Business Type is required'),
  sort_code: Yup.string().required('Sort Code is required'),
  start_date: Yup.string().required('Start Date is required'),
  vat_rate: Yup.string().required('Vat rate is required'),
});

export const StepThreeInitials = {
  account_number: '',
  business_type: '',
  sort_code: '',
  start_date: '',
  vat_rate: '',
};
export const StepThreeFields = [
  {
    title: 'Business Information',
    fields: [
      {
        name: 'business_type', label: 'Business Type', type: 'select', options: [
          { value: 'solo_trader', label: 'Sole Trader' },
          { value: 'partnership', label: 'Partnership' },
        ]
      },
      { name: 'account_number', label: 'Account Number', type: 'number' },
      { name: 'sort_code', label: 'Sort Code', type: 'number' },
      { name: 'vat_rate', label: 'Vat Rate', type: 'number' },
      {
        name: 'start_date', label: 'State Date', type: 'radio', options: [
          { value: 'before', label: 'before 6 sep' },
          { value: 'after', label: 'after 6 sep' }
        ]
      },
    ]
  },
];

export const stepFourValidation = Yup.object().shape({
  first_heard: Yup.string().required('First heard is required'),
  notes: Yup.string().required('Notes is required'),
});

export const StepFourInitials = {
  first_heard: '',
  notes: ''
};
export const StepFourFields = [
  {
    title: 'Others',
    fields: [
      {
        name: 'first_heard', label: 'First Heard', type: 'select', options: [
          { value: 'online_search', label: 'Online Search' },
          { value: 'facebook', label: 'Facebook' },
          { value: 'linkedin', label: 'Linkedin' },
          { value: 'instagram', label: 'Instagram' },
        ]
      },
      { name: 'notes', label: 'Notes', type: 'text' },
    ]
  }
];