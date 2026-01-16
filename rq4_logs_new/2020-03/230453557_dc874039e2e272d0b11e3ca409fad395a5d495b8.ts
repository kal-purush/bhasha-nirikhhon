import { pick } from "lodash";
import {
  validateName,
  validateEmail,
  validatePassword,
} from "validations-fiuba-laboral-v2";

const validator = (value: string, callback: (x: string) => void) => {
  try {
    callback(value);
  } catch ({ message }) {
    return message;
  }
};

interface IPropsToValidate {
  email: string;
  password: string;
  name: string;
  surname: string;
  padron: number;
  careers: any;
}

const validations = (values: IPropsToValidate) => {
  const email = validator(values.email, validateEmail);
  const password = validator(values.password, validatePassword);
  const name = validator(values.name, validateName);
  const surname = validator(values.surname, validateName);
  const padron = values.padron > 0 ? undefined : "El padron es mayor que 0";
  const careers = values.careers.length > 0 ? undefined : "Debes elegir una carrera";

  return {
    ...(email) && {email},
    ...(password) && {password},
    ...(name) && {name},
    ...(surname) && {surname},
    ...(padron) && {padron},
    ...(careers) && {careers}
  };
};

const signUpParams = (values: any) => pick(values, ["email", "password"]);

const saveApplicantParams = (values: any) => pick(values, ["name", "surname", "padron", "careers"]);

const translationsMapper = (trasnlations: string[] = Array(10).fill("")) => {
  const [
    title,
    email,
    password,
    name,
    surname,
    padron,
    careersTitle,
    credits,
    addCareerBtn,
    submit
  ] = trasnlations;

  return {
    title,
    email,
    password,
    name,
    surname,
    padron,
    careersTitle,
    credits,
    addCareerBtn,
    submit
  };
};

interface ICareer {
  code: string;
  description: string;
  credits: string;
}

const careersMapper = (careers: ICareer[]) =>
  careers.map(({code, description}) => ({ value: code, label: description}));

export { validations, signUpParams, saveApplicantParams, translationsMapper, careersMapper };