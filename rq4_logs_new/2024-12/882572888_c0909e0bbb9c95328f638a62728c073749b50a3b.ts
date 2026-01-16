import * as Yup from "yup";

export const SignUpSchema = Yup.object().shape({
  firstName: Yup.string().required("Firstname is required"),
  lastName: Yup.string().required("Lastname is required"),
  email: Yup.string().email("Invalid email").required("email is required"),
  password: Yup.string()
    .min(3, "Password must be at least 3 character")
    .required("Password is required"),
  confPassword: Yup.string()
    .oneOf([Yup.ref("password")], "Password must watch")
    .required("Confirmation password is required"),
});