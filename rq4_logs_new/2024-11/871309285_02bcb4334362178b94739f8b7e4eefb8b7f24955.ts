'use server';

import * as z from 'zod';
import { createDay } from '../day-service';
import { revalidatePath } from 'next/cache';

const createDaySchema = z.object({
  weekId: z.string().min(1, 'Week ID is required'),
  name: z.string().min(1, 'Name is required'),
});

export const createDayAction = async (_: unknown, formData: FormData) => {
  try {
    const day = await createDaySchema.parseAsync({
      weekId: formData.get('weekId'),
      name: formData.get('name'),
    });

    // Call the API to create a new day
    await createDay(day);

    // Revalidate the page
    revalidatePath('/programs/edit/[id]', 'page');
    return { success: true };
  } catch (e) {
    if (e instanceof z.ZodError) {
      return e.errors;
    }
    return {
      success: false,
      message:
        e != null && typeof e === 'object' && 'message' in e ? e.message : 'An error occurred',
    };
  }
};