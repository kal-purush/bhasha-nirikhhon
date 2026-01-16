import { useMutation } from "@tanstack/react-query"
import { useNavigate } from "react-router-dom"
import { authService } from "../services/auth"
import type { PasswordResetConfirmRequest } from "../services/auth"

export const usePasswordResetConfirm = () => {
  const navigate = useNavigate()

  return useMutation({
    mutationFn: async (data: PasswordResetConfirmRequest) => {
      return await authService.confirmPasswordReset(data)
    },
    onSuccess: () => {
      navigate("/login")
    },
  })
}