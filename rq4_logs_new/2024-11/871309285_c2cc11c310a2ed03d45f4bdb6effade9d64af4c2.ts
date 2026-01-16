'use server';

import * as z from 'zod';
import { createWeek } from '@/programs/week/week-service';
import { revalidatePath } from 'next/cache';
import type { Week } from '@prisma/client';

type CustomError = {
  success: false;
  message: string;
};
type State =
  | {
      success: true;
      week: Week;
    }
  | Array<z.ZodIssue>
  | CustomError;

const weekSchema = z.object({
  programId: z.string().min(1, 'Program ID is required'),
  name: z.string().min(1, 'Name is required'),
});

export default async function addWeekAction(
  currentState: State | null,
  data: FormData
) {
  try {
    const week = weekSchema.parse({
      programId: data.get('programId'),
      name: data.get('name'),
    });

    const newWeek = await createWeek(week);
    revalidatePath('/programs/edit/[id]');
    return { week: newWeek, success: true };
  } catch (e) {
    if (e instanceof z.ZodError) {
      return e.errors;
    }
    return {
      success: false,
      message:
        e != null && typeof e === 'object' && 'message' in e
          ? e.message
          : 'An error occurred',
    };
  }
}