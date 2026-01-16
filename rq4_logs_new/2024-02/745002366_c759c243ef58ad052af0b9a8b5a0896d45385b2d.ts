import * as yup from 'yup'
import { useForm } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

export type AuthFormProps = {
  email: string
  username: string
  password: string
}

const authFormSchema: yup.ObjectSchema<AuthFormProps> = yup.object({
  email: yup.string().required('Email is required').email('This is not a valid email'),
  username: yup.string().required('Username is required'),
  password: yup.string().required('Password is required'),
})

export const useRegisterForm = (defaultValues: AuthFormProps = { email: '', password: '', username: '' }) => {
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