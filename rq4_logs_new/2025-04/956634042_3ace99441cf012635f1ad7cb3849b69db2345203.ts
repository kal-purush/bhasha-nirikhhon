const API_URL = "http://127.0.0.1:8000"

export interface RegisterData {
  name: string
  last_name: string
  email: string
  password: string
}

export const authService = {
  register: async (data: RegisterData) => {
    try {
      console.log("Sending register request:", data)
      const response = await fetch(`${API_URL}/usuario/api/register/`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(data),
        credentials: "include",
        mode: "cors",
      })

      console.log("Response status:", response.status)
      const responseData = await response.json()
      console.log("Response data:", responseData)

      if (!response.ok) {
        throw new Error(responseData.message || "Registration failed")
      }

      return responseData
    } catch (error) {
      console.error("Register error:", error)
      throw error
    }
  },
}