import * as yup from "yup";

export const classSchema = yup.object().shape({
  code: yup.string().required(),
  teacher: yup.string().required(),
  students: yup
    .array()
    .of(
      yup.object({
        id: yup.string(),
        tests: yup.array().of(
          yup.object({
            test_id: yup.string(),
            reports: yup.array(),
            submiited: yup.bool(),
          })
        ),
      })
    )
    .default(function () {
      return [];
    }),
  timestamp: yup.date().default(function () {
    return +new Date();
  }),
});