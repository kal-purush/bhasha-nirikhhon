// Validation Messages.
export const VALIDATION_MESSAGES = {
    firstNameRequired: "First Name is Required!",
    firstNameAlpha: "First Name must contain only Alphabets!",
    firstNameMin: "First Name must be at least 6 characters long!",
    firstNameMax: "First Name must be at most 10 characters long!",
  
    lastNameRequired: "Last Name is Required!",
    lastNameAlpha: "Last Name must contain only Alphabets!",
    lastNameMin: "Last Name must be at least 6 characters long!",
    lastNameMax: "Last Name must be at most 10 characters long!",
  
    emailAddressRequired: "Email Address is Required!",
    emailAddressFormat: "Email Address must be in the Format: username@domain.tld",
    emailAddressPattern: "Email Address must be in the Format: username@domain.tld",
  
    phoneNumberRequired: "Phone Number is Required!",
    phoneNumberPattern: "Phone Number must be a Valid Sri Lankan Phone Number!",
  
    genderRequired: "Gender is Required!",
    genderAllowedValues: "Gender must be either 'Male' or 'Female'!",
  };
  
  // Gender Values.
  export const GENDER_VALUES = {
    male: "Male",
    female: "Female",
  };
  
  // Pagination & Sorting Defaults.
  export const PAGINATION_DEFAULTS = {
    page: 1,
    limit: 12,
    sort: "asc",
    sortBy: "firstName",
  };
  
  // HTTP Status Codes.
  export const STATUS_CODES = {
    ok: 200,
    created: 201,
    badRequest: 400,
    notFound: 404,
    internalServerError: 500,
  };
  
  // Logger Messages.
  export const LOGGER_MESSAGES = {
    employeeCreateSuccess: (id) => `New Employee Created with ID: ${id}\n`,
    employeeUpdateSuccess: (id) => `Updated Employee with ID: ${id}\n`,
    employeeDeleteSuccess: (id) => `Deleted Employee with ID: ${id}\n`,
    employeeFetchSuccess: (id) => `Fetched Employee with ID: ${id}\n`,
    employeeFetchError: (error) => `Error Fetching Employee: ${error}\n`,
    employeeCreateError: (error) => `Error Creating Employee: ${error}\n`,
    employeeUpdateError: (error) => `Error Updating Employee: ${error}\n`,
    employeeDeleteError: (error) => `Error Deleting Employee: ${error}\n`,
    employeeValidationFailed: (errors) => `Validation Failed:\n${JSON.stringify(errors, null, 2)}\n`,
    employeesFetchError: (error) => `Error Fetching Employees: ${error}\n`,
  };
  