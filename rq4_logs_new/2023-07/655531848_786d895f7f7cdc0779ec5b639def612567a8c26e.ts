import { FormikValues } from 'formik'

import { Fields, FormProps } from '@use-form/useForm.types'

export const getFieldProps = <D extends FormikValues>({
  values,
  errors,
  touched,
}: FormProps<D>): Fields<D> =>
  Object.keys(values).reduce((acc, fieldName) => {
    acc[fieldName as keyof D] = {
      value: values[fieldName],
      error:
        errors[fieldName] && touched[fieldName] ? errors[fieldName] : undefined,
      touched: touched[fieldName],
    }
    return acc
  }, {} as Fields<D>)