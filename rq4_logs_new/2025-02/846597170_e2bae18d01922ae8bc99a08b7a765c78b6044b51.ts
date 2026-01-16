import * as yup from 'yup';

export const adminLoginSchema = yup.object().shape({
  email: yup.string().email('Invalid email format').required('Email is required'),
  password: yup.string().min(6, 'Password must be at least 6 characters').required('Password is required'),
});

export const adminSignUpSchema = yup.object().shape({
  firstName: yup.string().required('First name is required'),
  lastName: yup.string().required('Last name is required'),
  email: yup.string().email('Invalid email format').required('Email is required'),
  password: yup.string().min(6, 'Password must be at least 6 characters').required('Password is required'),
  agreeToTerms: yup.boolean().oneOf([true], 'You must agree to the terms and conditions').required('Agreement to terms is required'),
  isAdmin: yup.boolean().required('Admin status is required'),
});