import { RegisterOptions } from 'react-hook-form'

import { josa } from 'es-hangul'

export type FormField =
  | 'email'
  | 'password'
  | 'passwordConfirmation'
  | 'name'
  | 'nickname'
  | 'introduce'
  | 'github'

const FIELD_DICTIONARY: Record<FormField, string> = {
  email: '이메일',
  password: '비밀번호',
  passwordConfirmation: '비밀번호 확인',
  name: '이름',
  nickname: '닉네임',
  introduce: '소개',
  github: '깃허브',
}
const EMAIL_PATTERN = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/
const PASSWORD_PATTERN = /^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d!@#$%^&*]{8,16}$/
const MAX_PASSWORD_LENGTH = 16
const MIN_PASSWORD_LENGTH = 8
const MAX_NAME_LENGTH = 8
const MAX_NICKNAME_LENGTH = 10
const MAX_INTRODUCE_LENGTH = 100
const GITHUB_PATTERN = /^[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$/

export const generateRequiredMessage = (name: FormField): string => {
  return `${josa(FIELD_DICTIONARY[name], '을/를')} 입력해주세요.`
}

const EMAIL_RULES: RegisterOptions = {
  required: generateRequiredMessage('email'),
  pattern: {
    value: EMAIL_PATTERN,
    message: '올바른 이메일 형식이 아닙니다.',
  },

  // TODO: 이미 가입된 이메일 확인하는 로직 추가
}

const PASSWORD_RULES: RegisterOptions = {
  required: generateRequiredMessage('password'),
  minLength: {
    value: MIN_PASSWORD_LENGTH,
    message: `비밀번호는 최소 ${MIN_PASSWORD_LENGTH}자 이상입니다.`,
  },
  maxLength: {
    value: MAX_PASSWORD_LENGTH,
    message: `비밀번호는 최대 ${MAX_PASSWORD_LENGTH}자 이하입니다.`,
  },
  pattern: {
    value: PASSWORD_PATTERN,
    message:
      '영문 대소문자, 숫자 2가지 이상으로 조합해 8자 이상 16자 이하로 입력해주세요.',
  },
}

export const PASSWORD_CONFIRM_RULES = (
  passwordValue: string
): RegisterOptions => ({
  required: generateRequiredMessage('passwordConfirmation'),
  validate: value => value === passwordValue || '비밀번호가 일치하지 않습니다.',
})

const NAME_RULES: RegisterOptions = {
  required: generateRequiredMessage('name'),
  maxLength: {
    value: MAX_NAME_LENGTH,
    message: `이름은 최대 ${MAX_NAME_LENGTH}자까지 가능합니다.`,
  },
}

const NICKNAME_RULES: RegisterOptions = {
  maxLength: {
    value: MAX_NICKNAME_LENGTH,
    message: `닉네임은 최대 ${MAX_NICKNAME_LENGTH}자까지 가능합니다.`,
  },
}

const INTRODUCE_RULES: RegisterOptions = {
  maxLength: {
    value: MAX_INTRODUCE_LENGTH,
    message: `소개는 최대 ${MAX_INTRODUCE_LENGTH}자까지 가능합니다.`,
  },
}

const GITHUB_RULES: RegisterOptions = {
  pattern: {
    value: GITHUB_PATTERN,
    message: '올바른 형식이 아닙니다.',
  },
}

export const VALIDATION_RULES: Record<FormField, RegisterOptions> = {
  email: EMAIL_RULES,
  password: PASSWORD_RULES,
  passwordConfirmation: {},
  name: NAME_RULES,
  nickname: NICKNAME_RULES,
  introduce: INTRODUCE_RULES,
  github: GITHUB_RULES,
}