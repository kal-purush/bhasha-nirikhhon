import { Resend } from 'resend';

import { EmailTemplate } from '@/features/email/components/EmailTemplate';

const resend = new Resend(process.env.RESEND_API_KEY);

const errorResponse = (error: unknown) =>
  Response.json({ error }, { status: 500 });

export async function POST() {
  try {
    const { data, error } = await resend.emails.send({
      // currently only works with @resend.dev, presumably free tier constraint
      from: 'Courses <onboarding@resend.dev>',
      // works with my email in the "to" property
      to: ['test@testing.com'],
      subject: 'Courses Registration Confirmation',
      react: EmailTemplate({ firstName: 'Jeff' }),
    });

    return error ? errorResponse(error) : Response.json(data);
  } catch (error) {
    return errorResponse(error);
  }
}