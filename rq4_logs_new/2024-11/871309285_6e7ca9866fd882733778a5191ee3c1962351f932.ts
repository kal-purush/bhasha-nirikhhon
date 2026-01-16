'use server';
import * as z from 'zod';
import { deleteProgram } from '../program-service';
import { redirect } from 'next/navigation';
import { revalidatePath } from 'next/cache';

const deleteExerciseSchema = z.object({
  id: z.string().min(1, 'Id is required'),
});
export default async function deleteProgramAction(
  formData: FormData
): Promise<void> {
  try {
    const { id: parsedId } = deleteExerciseSchema.parse({
      id: formData.get('id'),
    });
    await deleteProgram(parsedId);
    revalidatePath('/programs');
  } catch (e) {
    console.log({ e });
  }
  redirect('/programs');
}