
import { type NextRequest, NextResponse } from 'next/server';
import { gradeTask } from '~/app/actions/grading ';
import { AnswerValue, QuestionType } from '~/app/components/QuestionForm';
import { AnswerSchema } from '~/utils/validation/answersSchema.validation';
import { QuestionSchema } from '~/utils/validation/schema.validation';



export async function POST(req: NextRequest) {
  try {
    const  {questions,userAnswers} = await req.json() as {questions:QuestionType[], userAnswers:Record<string, AnswerValue>};
    
    const task = await gradeTask(questions,userAnswers);
    const parsedQuestions = AnswerSchema.array().parse(task);
    if (!parsedQuestions) {
      throw new Error('Parsed questions are undefined or null');
  }
    return NextResponse.json(
      { message: 'Here is the gades from AI', data: parsedQuestions},
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