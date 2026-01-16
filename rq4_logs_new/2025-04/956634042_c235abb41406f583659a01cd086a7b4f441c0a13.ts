import { useMutation } from "@tanstack/react-query"
import { useNavigate } from "react-router-dom"
import { authService } from "../services/auth"
import { useUserStore } from "../store/useUserStore"

export const useLogout = () => {
  const navigate = useNavigate()
  const clearUsername = useUserStore((state) => state.clearUsername)

  return useMutation({
    mutationFn: () => authService.logout(),
    onSuccess: () => {
      clearUsername()
      navigate("/login")
    },
    onError: (error) => {
      console.error("Logout error:", error)
    },
  })
}