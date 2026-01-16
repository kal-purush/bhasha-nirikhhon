import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { testRoute } from './common.js';

const mockQuestions = [
  {
    questionId: '1',
    description: 'What is your first name?',
    type: 'small-text',
    onboarding_screen: 1
  },
  {
    questionId: '2',
    description: 'What is your last name?',
    type: 'small-text',
    onboarding_screen: 1
  }
];

// Happy Path Tests
// ============================================================================

// /events/all
testRoute({
  description: 'returns all events',
  reqData: {},
  mockData: mockQuestions,
  expectData: {
    status: 200,
    body: mockQuestions
  },
  model: 'question',
  route: 'all'
});

// /events/:id
testRoute({
  description: 'returns event matching :id',
  reqData: {},
  mockData: mockQuestions.filter((question) => question.questionId === '1'),
  expectData: {
    status: 200,
    body: mockQuestions.filter((question) => question.questionId === '1'),
    calledWith: {
      where: {
        questionId: ':id'
      }
    }
  },
  model: 'question',
  id: '1'
});

// Sad Path Tests
// ============================================================================

// /events/all
testRoute({
  description: 'returns no events if there are none',
  reqData: {},
  mockData: {},
  expectData: {
    status: 200,
    body: {}
  },
  model: 'question',
  route: 'all'
});

// /events/:id
testRoute({
  description: 'returns nothing if no event matches :id',
  reqData: {},
  mockData: {},
  expectData: {
    status: 200,
    body: {},
    calledWith: {
      where: {
        questionId: ':id'
      }
    }
  },
  model: 'question',
  id: '5'
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
// /events/all
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
    }
  },
  model: 'question',
  route: 'all'
});

// /events/:id
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
      where: {
        questionId: ':id'
      }
    }
  },
  model: 'question',
  id: '1'
});