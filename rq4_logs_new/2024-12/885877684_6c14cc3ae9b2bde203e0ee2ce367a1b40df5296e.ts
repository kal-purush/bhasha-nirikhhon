import { z } from 'zod';

const changePasswordSchema = z.object({
  currentPassword: z.string().min(1, "Current password is required"),
  newPassword: z.string().min(8, "New password must be at least 8 characters"),
  confirmNewPassword: z.string().min(8, "Confirm new password must be at least 8 characters"),
}).refine((data) => data.newPassword === data.confirmNewPassword, {
  message: "New passwords don't match",
  path: ["confirmNewPassword"],
});

type ChangePasswordRequest = z.infer<typeof changePasswordSchema>;

interface ChangePasswordResponse {
  status: string;
  message: string;
}

export async function changePassword(
  userId: number,
  currentPassword: string,
  newPassword: string,
  confirmNewPassword: string
): Promise<ChangePasswordResponse> {
  try {
    const validatedData = changePasswordSchema.parse({
      currentPassword,
      newPassword,
      confirmNewPassword,
    });
    console.log('alpi fil', userId)

    // Update the API endpoint to match your backend route
    const response = await fetch(`http://localhost:4000/api/auth/change-password/${userId}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        currentPassword: validatedData.currentPassword,
        newPassword: validatedData.newPassword,
        confirmNewPassword: validatedData.confirmNewPassword
      }),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({
        message: 'Failed to change password. Please try again.'
      }));
      throw new Error(errorData.message || 'Failed to change password');
    }

    const data: ChangePasswordResponse = await response.json();
    return data;
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new Error(error.errors[0].message);
    }
    if (error instanceof Error) {
      throw error;
    }
    throw new Error('An unexpected error occurred');
  }
}
