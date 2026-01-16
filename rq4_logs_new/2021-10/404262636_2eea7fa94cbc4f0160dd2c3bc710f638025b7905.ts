import * as Yup from "yup";

const validationSchema = Yup.object().shape({
  title: Yup.string().required("This field is required"),
  release_date: Yup.date().nullable().required("This field is required"),
  poster_path: Yup.string().required("This field is required"),
  genres: Yup.array()
    .min(1, "Pick at least 1 genre")
    .of(
      Yup.object().shape({
        label: Yup.string().required(),
        value: Yup.string().required(),
      })
    ),
  overview: Yup.string().required("This field is required"),
  runtime: Yup.number()
    .required("This field is required")
    .typeError("Field value should be a number")
    .positive("Should be greater than zero"),
});

export default validationSchema;