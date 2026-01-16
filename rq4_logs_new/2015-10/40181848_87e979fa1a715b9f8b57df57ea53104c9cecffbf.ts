class RequiredStringValidator implements IValidator<string> {
  constructor(public message: string = 'Field is required') {
  }
  
  isValid(value: string): boolean {
    return !!value;
  }
}

class PasswordValidator implements IValidator<string> {
  constructor(public message: string = 'Password is required') {
  }
  
  isValid(value: string): boolean {
    if (!new RequiredStringValidator().isValid(value)) {
      return false; 
    }
    
    if (value.length < 6) {
      this.message = 'Password should be greater than 6 characters';
      return false;
    }
    
    return true;
  }
}

export default {
  RequiredStringValidator,
  PasswordValidator
};