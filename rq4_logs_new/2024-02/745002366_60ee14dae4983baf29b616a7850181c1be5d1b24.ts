import * as yup from 'yup'
import { useForm } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

export type AuthFormProps = {
  email: string
  password: string
}

const authFormSchema: yup.ObjectSchema<AuthFormProps> = yup.object({
  email: yup.string().required('Email is required').email('This is not a valid email'),
  password: yup.string().required('Password is required'),
})

export const useAuthForm = (defaultValues: AuthFormProps = { email: '', password: '' }) => {
  const { register, control, handleSubmit, formState, setValue } = useForm<AuthFormProps>({
    defaultValues: defaultValues,
    resolver: yupResolver(authFormSchema),
  })

  return {
    register,
    handleSubmit,
    control,
    setValue,
    errors: formState.errors,
    values: formState.defaultValues,
  }
}