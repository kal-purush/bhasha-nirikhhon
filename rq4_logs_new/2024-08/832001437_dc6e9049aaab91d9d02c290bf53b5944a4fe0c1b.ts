'use server';
import { stepOneSchema } from '../_component/schemas';
import { OnBoardingRoutes, FormErrors } from '../_component/path-types';
import { redirect } from 'next/navigation';

export default async function stepOneFormAction(prevState: FormErrors | undefined, formData: FormData) {
  const data = Object.fromEntries(formData.entries());
  const validated = stepOneSchema.safeParse(data);

  if (!validated.success) {
    const errors = validated.error.issues.reduce((acc: FormErrors, issue) => {
      const path = issue.path[0] as string;
      acc[path] = issue.message;
      return acc;
    }, {});
    return errors;
  }

  return {
    message: 'Please enter a valid email',
  };
}