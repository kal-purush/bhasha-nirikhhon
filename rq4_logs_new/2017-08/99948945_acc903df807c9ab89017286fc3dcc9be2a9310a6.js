const { Validator } = require("jsonschema");

const getErrors = errors => {
  return errors.map(val => {
    return {
      value: val.instance,
      property: val.property.split(".")[1],
      message: val.message
    };
  });
};

const validate = (data, schema) => {
  const v = new Validator();
  const res = v.validate(data, schema);

  if (!res.valid) {
    return {
      isValid: false,
      errors: getErrors(res.errors)
    };
  }

  return { isValid: true };
};

module.exports = {
  validate
};