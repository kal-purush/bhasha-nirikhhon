import { useMutation } from "@tanstack/react-query"
import { useAuthStore } from "../store/authStore"

const API_URL = "http://127.0.0.1:8000"

interface UpdateAvatarResponse {
  success: boolean
  message: string
}

export const useUpdateAvatar = () => {
  const accessToken = useAuthStore((state) => state.accessToken)
  const setAvatarId = useAuthStore((state) => state.setAvatarId)

  return useMutation<UpdateAvatarResponse, Error, number>({
    mutationFn: async (avatarId: number) => {
      const response = await fetch(`${API_URL}/usuario/api/update-avatar/`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify({ avatar_id: avatarId }),
        credentials: "include",
      })

      const responseData = await response.json()

      if (!response.ok) {
        throw new Error(responseData.message || "Failed to update avatar")
      }

      return responseData
    },
    onSuccess: (_, avatarId) => {
      setAvatarId(avatarId.toString())
    },
  })
}