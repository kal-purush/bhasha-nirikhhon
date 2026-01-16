import * as Yup from 'yup';

const SignUpInvestorValidationSchema = Yup.object().shape({
  email: Yup.string()
    .email('Please enter a valid email')
    .required('Please enter email'),
  password: Yup.string()
    .required('Please enter password')
    .min(8, 'Passowrd must contain atleast 8 letter'),
  confirmPassword: Yup.string()
    .required('Please re-enter password')
    .min(8, 'Passowrd must contain atleast 8 letter'),
  mobileNumber: Yup.string().required('Please enter mobileNumber'),
});

const SignUpCompanyValidationSchema = Yup.object().shape({
  founderName: Yup.string().required('Please enter founderName'),
  companyName: Yup.string().required('Please enter companyName'),
  phoneNumber: Yup.string().required('Please enter phoneNumber'),
  email: Yup.string()
    .email('Please enter a valid email')
    .required('Please enter email'),
});

export { SignUpInvestorValidationSchema, SignUpCompanyValidationSchema };