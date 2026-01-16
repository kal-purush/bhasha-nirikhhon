import { useMutation, useQueryClient } from "@tanstack/react-query";
import { RegisterDto } from "@/lib/routes/auth/dto/register.dto";
import { LoginDto } from "@/lib/routes/auth/dto/login.dto";
import { AuthRouter } from "@/lib/routes/auth";

// Hook for Register Mutation
export function useRegisterMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (data: RegisterDto) => AuthRouter.register(data),
    onSuccess: (data) => {
      console.log("Registration successful", data);
      queryClient.invalidateQueries({ queryKey: ["user"] }); // Refresh user data
    },
    onError: (error) => {
      console.error("Registration failed", error);
    },
  });
}

// Hook for Login Mutation
export function useLoginMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (data: LoginDto) => AuthRouter.login(data),
    onSuccess: (data) => {
      console.log("Login successful", data);
      queryClient.invalidateQueries({ queryKey: ["user"] }); // Refresh user state
    },
    onError: (error) => {
      console.error("Login failed", error);
    },
  });
}

// Hook for Confirm Account Mutation
export function useConfirmAccountMutation() {
  return useMutation({
    mutationFn: async (data: { username: string; code: string }) =>
      AuthRouter.confirm(data),
    onSuccess: (data) => {
      console.log("Account confirmed", data);
    },
    onError: (error) => {
      console.error("Account confirmation failed", error);
    },
  });
}

// Hook for Refresh Token Mutation
export function useRefreshTokenMutation() {
  return useMutation({
    mutationFn: async (data: { refreshToken: string }) =>
      AuthRouter.refresh(data),
    onSuccess: (data) => {
      console.log("Token refreshed", data);
    },
    onError: (error) => {
      console.error("Token refresh failed", error);
    },
  });
}