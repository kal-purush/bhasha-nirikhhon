import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { testRoute } from './common.js';

const medicine = {
  medicineId: 'testID_1234',
  name: 'testName',
  userId: '3'
};

// Happy Path Tests
// ============================================================================

// /medicines/create
const { userId, ...medicineProps } = medicine;
testRoute({
  description: 'returns created medicine object',
  reqData: {
    ...medicine
  },
  mockData: medicine,
  expectData: {
    status: 200,
    body: medicine,
    calledWith: {
      data: {
        ...medicineProps,
        user: { connect: { userId: medicine.userId } }
      }
    }
  },
  model: 'medicine',
  route: 'create'
});

// Sad Path Tests
// ============================================================================

// /medicines/create
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
        name: undefined,
        medicineId: undefined
      }
    }
  },
  model: 'medicine',
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

// /medicines/create
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
        name: undefined,
        medicineId: undefined
      }
    }
  },
  model: 'medicine',
  route: 'create'
});