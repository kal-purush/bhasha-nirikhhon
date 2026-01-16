export const ID_ERROR_MESSAGES = {
  REQUIRED_ERROR: "필수 입력 항목입니다.",
  INVALID_TYPE_ERROR: "올바른 타입이 아닙니다.",
  REGEX_ERROR: "ID는 영문자로 시작해야 하며, 영문과 숫자만 포함할 수 있습니다.",
  ALREADY_EXIST_ERROR: "이미 사용 중인 ID입니다.",
  MIN_LENGTH_ERROR: (min = 4) => `ID는 최소 ${min}자 이상이어야 합니다.`,
  MAX_LENGTH_ERROR: (max = 20) => `ID는 최대 ${max}자 이하이어야 합니다.`,
} as const;

export const PASSWORD_ERROR_MESSAGES = {
  REQUIRED_ERROR: "필수 입력 항목입니다.",
  INVALID_TYPE_ERROR: "올바른 타입이 아닙니다.",
  REGEX_ERROR:
    "비밀번호는 영문자와 숫자 또는 특수문자(!@#$%^&*) 중 최소 두 가지 이상을 포함해야 하며, 공백을 포함할 수 없습니다.",
  NOT_CONFIRMED_ERROR: "비밀번호가 일치하지 않습니다.",
  MIN_LENGTH_ERROR: (min = 8) => `비밀번호는 최소 ${min}자 이상이어야 합니다.`,
  MAX_LENGTH_ERROR: (max = 20) => `비밀번호는 최대 ${max}자 이하이어야 합니다.`,
} as const;

export const NICKNAME_ERROR_MESSAGES = {
  REQUIRED_ERROR: "필수 입력 항목입니다.",
  INVALID_TYPE_ERROR: "올바른 타입이 아닙니다.",
  REGEX_ERROR:
    "닉네임은 한글, 영문, 숫자만 포함할 수 있으며, 공백을 포함할 수 없습니다.",
  ALREADY_EXIST_ERROR: "이미 사용 중인 닉네임입니다.",
  MIN_LENGTH_ERROR: (min = 2) => `닉네임은 최소 ${min}자 이상이어야 합니다.`,
  MAX_LENGTH_ERROR: (max = 16) => `닉네임은 최대 ${max}자 이하이어야 합니다.`,
} as const;