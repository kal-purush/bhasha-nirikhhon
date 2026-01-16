const passwordStrength = require('check-password-strength');

const isEmailValid = (email: string) => {
  const re = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  return re.test(String(email).toLowerCase());
};

const isNameValid = (name: string) => {
  const re = /^[a-zA-Z]+(?:-[a-zA-Z]+)*$/;
  return re.test(String(name).toLowerCase());
};

const checkPasswordStrength = (password: string) => {
  return passwordStrength(password).value;
};

const isPasswordWeak = (password: string) => {
  return checkPasswordStrength(password) === 'Weak';
};

const isPasswordMedium = (password: string) => {
  return checkPasswordStrength(password) === 'Medium';
};

const isPasswordStrong = (password: string) => {
  return checkPasswordStrength(password) === 'Strong';
};

export default {
  isEmailValid,
  isNameValid,
  checkPasswordStrength,
  isPasswordWeak,
  isPasswordMedium,
  isPasswordStrong
};