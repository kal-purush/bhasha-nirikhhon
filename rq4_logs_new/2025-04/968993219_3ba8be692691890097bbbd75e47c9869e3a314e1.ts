import { z } from 'zod';

export const registerSchema = z.object({
  fullname: z.string().min(1, 'Please enter your fullname'),
  email: z.string().email('Invalid email'),
  password: z.string().min(8, 'Password must be at least 8 characters'),
  learnSkills: z.array(z.string()).min(1, 'Please select at least one skill you want to learn'),
  teachSkills: z.array(z.string()).min(1, 'Please select at least one skill you want to teach'),
});

export type RegisterFormData = z.infer<typeof registerSchema>;

