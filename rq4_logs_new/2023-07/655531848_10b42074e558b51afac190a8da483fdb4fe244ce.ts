import { useEffect } from 'react'

import { useNavigate } from 'react-router-dom'

import { useForm } from '@hooks'
import { validate } from '@utils/validate'
import { SignupSchema } from '@pages/signup/SignupSchema'

export const useSignup = () => {
  const { formik, status } = useForm({
    url: 'http://localhost:3000/api/auth/sign-up',
    initialValues: {
      username: '',
      password: '',
      passwordConfirm: '',
    },
    validate: validate(SignupSchema),
    initialTouched: {
      username: false,
      password: false,
      passwordConfirm: false,
    },
  })
  const navigate = useNavigate()

  useEffect(() => {
    if (status === 'success') navigate('/auth/login')
  }, [status, navigate])

  return { formik, status }
}