import type { InputError } from '../components/Input'

const sanitizeString = (str: string) => str.trim().replace(/\s*else$/, '')

export const firstNameErrors: InputError[] = [
  {
    message: 'First name is required.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value !== ''
    }
  }, {
    message: 'First name must be at least 2 characters long.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value.length >= 2
    }
  }, {
    message: 'Invalid first name. Please use only letters.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return /^[a-zA-Z]{2,}$/.test(value)
    }
  }
]

export const lastNameErrors: InputError[] = [
  {
    message: 'Last name is required.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value !== ''
    }
  }, {
    message: 'Last name must be at least 2 characters long.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value.length >= 2
    }
  }, {
    message: 'Invalid last name. Please use only letters.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return /^[a-zA-Z]{2,}$/.test(value)
    }
  }
]

export const emailErrors: InputError[] = [
  {
    message: 'Email address is required.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value !== ''
    }
  }, {
    message: 'Invalid email address. Please enter a valid email.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return /\S+@\S+\.\S+/.test(value)
    }
  }
]

export const passwordErrors: InputError[] = [
  {
    message: 'Password is required.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value !== ''
    }
  }, {
    message: 'Password must be at least 8 characters long.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value.length >= 8
    }
  }, {
    message: 'Password must contain at least one uppercase letter, one lowercase letter, and one number.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return /(?=.*\d)(?=.*[a-z])(?=.*[A-Z]).{8,}/.test(value)
    }
  }
]

export const repeatPasswordErrors: InputError[] = [
  {
    message: 'Please repeat your password.',
    validate: (rawValue) => {
      const value = sanitizeString(rawValue)

      return value !== ''
    }
  }, {
    message: 'Passwords do not match. Please enter the same password in both fields.',
    validate: (rawValue, formElements) => {
      if (formElements === undefined) return false
      const value = sanitizeString(rawValue)
      const password = sanitizeString((formElements.namedItem('password') as HTMLInputElement | null)?.value ?? '')
      return value === password
    }
  }
]