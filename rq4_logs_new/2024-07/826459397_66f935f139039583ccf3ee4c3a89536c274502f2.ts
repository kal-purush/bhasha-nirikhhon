import * as yup from "yup";

const validationSchema = yup.object().shape({
  firstname: yup.string().required("First name is required"),
  lastname: yup.string().required("Last name is required"),
  username: yup.string().required("Username is required"),
  password: yup
    .string()
    .required("Password is required")
    .min(8, "Password must be at least 8 characters")
    .matches(/[a-zA-Z]/, "Password must contain both letters and numbers")
    .matches(/[0-9]/, "Password must contain both letters and numbers"),
  phoneNumber: yup
    .string()
    .required("Phone number is required")
    .min(11, "Phone number must be 11 characters"),
  // .matches(/^[0-9]+$/, "Phone number must be numeric"),
  address: yup.string().required("Address is required"),
});

export default validationSchema;