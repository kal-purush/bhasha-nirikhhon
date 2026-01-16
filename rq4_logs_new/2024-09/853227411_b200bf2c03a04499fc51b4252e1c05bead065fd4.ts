
import { type NextRequest, NextResponse } from 'next/server';
import { QuestionSchema } from '~/utils/validation/schema.validation';
import { createPathTask } from '~/app/actions';


export async function POST(req: NextRequest) {
  try {
    const  topic = await req.json();
    
    const task = await createPathTask(topic);
    const parsedQuestions = QuestionSchema.array().parse(task);
    if (!parsedQuestions) {
        throw new Error('Parsed questions are undefined or null');
    }
    return NextResponse.json(
      { message: 'Here is the response from AI', data: parsedQuestions},
      { status: 201 }
    );
  } catch (error: unknown) {
    console.error('Error:', error);
    return NextResponse.json(
      { message: 'Failed to query AI', error: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}