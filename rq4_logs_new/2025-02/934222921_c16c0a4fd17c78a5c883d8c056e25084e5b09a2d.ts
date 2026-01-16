import * as Yup from 'yup';

export const RegisterValidates = Yup.object().shape({
  fullName: Yup.string()
    .matches(/^\S+\s+\S+/, 'Debes ingresar nombre y apellido')
    .required('Campo requerido'),

  email: Yup.string().email('Correo inválido').required('Campo requerido'),

  password: Yup.string()
    .min(8, 'Mínimo 8 caracteres')
    .matches(/[A-Z]/, 'Debe contener una mayúscula')
    .matches(/[a-z]/, 'Debe contener una minúscula')
    .matches(/[!@#$%^&*]/, 'Debe contener un carácter especial')
    .required('Campo requerido'),

  confirmPassword: Yup.string()
    .oneOf([Yup.ref('password')], 'Las contraseñas no coinciden')
    .required('Campo requerido'),

  phone: Yup.string()
    .matches(/^\d{10}$/, 'Debe tener 10 dígitos')
    .required('Campo requerido'),

  address: Yup.string().required('Campo requerido'),
});

export default RegisterValidates;