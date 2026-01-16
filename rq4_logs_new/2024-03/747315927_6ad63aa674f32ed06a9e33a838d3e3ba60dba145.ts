import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { testRoute } from './common.js';

const answer = {
  answerId: 'testID_1234',
  answer_text: 'test answer',
  questionId: '1',
  userId: '3',
  babyId: '3'
};

// Happy Path Tests
// ============================================================================

// /answers/create
const { userId, babyId, questionId, ...answerProps } = answer;
testRoute({
  description: 'returns created answer object',
  reqData: {
    ...answer
  },
  mockData: answer,
  expectData: {
    status: 200,
    body: answer,
    calledWith: {
      data: {
        ...answerProps,
        user: { connect: { userId: answer.userId } },
        question: { connect: { questionId: answer.questionId } },
        baby: { connect: { babyId: answer.babyId } }
      }
    }
  },
  model: 'answer',
  route: 'create'
});

// Sad Path Tests
// ============================================================================

// /answers/create
testRoute({
  description: 'returns empty object when no data passed',
  reqData: {},
  mockData: {},
  expectData: {
    status: 200,
    body: {},
    calledWith: {
      data: {
        user: { connect: { userId: undefined } },
        question: { connect: { questionId: undefined } },
        baby: { connect: { babyId: undefined } },
        answerId: undefined,
        answer_text: undefined
      }
    }
  },
  model: 'answer',
  route: 'create'
});

// Error Path Tests
// ============================================================================

const prismaError = new PrismaClientKnownRequestError(
  'Error manually generated for testing',
  {
    code: 'P2010',
    clientVersion: 'jest mock'
  }
);

// /answers/create
testRoute({
  description: 'returns HTTP 500 if Prisma throws error',
  reqData: {},
  mockData: prismaError,
  expectData: {
    status: 500,
    body: {
      name: prismaError.name,
      code: prismaError.code,
      clientVersion: prismaError.clientVersion
    },
    calledWith: {
      data: {
        user: { connect: { userId: undefined } },
        question: { connect: { questionId: undefined } },
        baby: { connect: { babyId: undefined } },
        answerId: undefined,
        answer_text: undefined
      }
    }
  },
  model: 'answer',
  route: 'create'
});