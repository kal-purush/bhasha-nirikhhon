"use server"

import { extenstion } from "@/utils/extensition"

export const server_action = async (_: any, data: FormData) => {
  const schema = {
    email: data.get("email"),
    phone_number: data.get("phone_number"),
    password: data.get("password"),
    password_confirm: data.get("password_confirm")
  }
  const result = extenstion.safeParse(schema)
  console.log(result)
  if (!result.success) {
    return result.error.flatten()
  }
  // return result
}
// prev argument 타입 찾아보기
/*
parse - zod의 유효성 검사를 통과하지 못하면 error가 발생한다.
safeParse - zod의 유효성 검사를 통과하지 못해도 error가 발생하지 않는다.
*/