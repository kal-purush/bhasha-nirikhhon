import { request } from "./utils"

export const authenticate = async (name: string, signature: string, address: string) => {
    try {
        const data = await request({
          url: "/auth/login",
          data: {
            signature,
            name,
            address
          },
          method: "POST",
        })

        return data.data
      } catch (e: any) {
        throw new Error(e.response.data)
      }
}