import { object, string, ref } from 'yup'


let userSchema = object({
  nombre: string().required("Ingresar nombre"),
  telefono: string().required("Ingresar telefono"),
  email: string()
    .email("el campo email no tiene el formato correcto")
    .required("Ingresar email"),
  confirmEmail: string()
    .oneOf([ref("email"), null], "Los correos electrónicos deben coincidir")
    .required("Confirmar email"),
});


const validateForm = async (datosForm) => {
  try {
    await userSchema.validate(datosForm);
    return{ status : "success" }
  } catch (error) {
    return { status: "error" , message: error.message }
  }
};

export default validateForm;