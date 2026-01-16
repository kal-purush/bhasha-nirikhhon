'use server';
import { z } from 'zod';
import { getUserUseCase } from '../_use-cases/get-user';
import { NeedAuthError } from '@/shared/lib/errors';
import { getSessionStrictServer } from '../session.server';

const propsSchema = z.object({
  userId: z.string(),
});

const profileSchema = z.object({
  email: z.string(),
  name: z.string().nullable().optional(),
  image: z.string().nullable().optional(),
});

const resultSchema = z.object({
  profile: profileSchema,
});

export const getUserProfileAction = async (
  props: z.infer<typeof propsSchema>
) => {
  const { userId } = propsSchema.parse(props);

  const session = await getSessionStrictServer();

  const user = await getUserUseCase.exec({
    session,
    userId,
  });

  return resultSchema.parseAsync({
    profile: user,
  });
};