import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { testRoute } from './common.js';

const mockSleepWindows = [
  {
    windowId: '1',
    eventId: '1',
    startTime: '2022-01-01T10:00:00.000Z',
    stopTime: '2022-01-01T12:00:00.000Z',
    isSleep: true,
    note: 'good sleep'
  },
  {
    windowId: '2',
    eventId: '1',
    startTime: '2022-01-01T12:00:00.000Z',
    stopTime: '2022-01-01T13:00:00.000Z',
    isSleep: false,
    note: 'cried a lot'
  },
  {
    windowId: '3',
    eventId: '1',
    startTime: '2022-01-01T13:00:00.000Z',
    stopTime: '2022-01-01T14:00:00.000Z',
    isSleep: true,
    note: 'bad sleep'
  }
];
const newSleepWindow = {
  windowId: '4',
  eventId: '1',
  startTime: '2022-01-01T14:00:00.000Z',
  stopTime: '2022-01-01T15:00:00.000Z',
  isSleep: false,
  note: 'baby cried so much'
};

// Happy Path Tests
// ============================================================================

// /sleep-windows/:id
testRoute({
  description: 'returns a single sleep window',
  reqData: {},
  mockData: mockSleepWindows.filter((window) => window.windowId === '1'),
  expectData: {
    status: 200,
    body: mockSleepWindows.filter((window) => window.windowId === '1'),
    calledWith: {
      where: {
        windowId: ':id'
      }
    }
  },
  model: 'sleep-window',
  id: '1'
});

// /sleep-windows/create
const { windowId: _e, eventId: _o, ...windowProps } = newSleepWindow;
testRoute({
  description: 'creates a new sleep window',
  reqData: {
    ...newSleepWindow
  },
  mockData: newSleepWindow,
  expectData: {
    status: 200,
    body: newSleepWindow,
    calledWith: {
      data: {
        ...windowProps,
        event: {
          connect: {
            eventId: newSleepWindow.eventId
          }
        }
      }
    }
  },
  model: 'sleep-window',
  route: 'create'
});

// /sleep-windows/update
testRoute({
  description: 'returns updated sleep window',
  reqData: {
    ...newSleepWindow
  },
  mockData: newSleepWindow,
  expectData: {
    status: 200,
    body: newSleepWindow,
    calledWith: {
      data: {
        ...newSleepWindow
      },
      where: {
        windowId: ':id'
      }
    }
  },
  model: 'sleep-window',
  id: '1',
  route: 'update'
});

// /sleep-windows/delete
testRoute({
  description: 'returns deleted sleep window',
  reqData: {},
  mockData: newSleepWindow,
  expectData: {
    status: 200,
    body: newSleepWindow,
    calledWith: {
      where: {
        windowId: ':id'
      }
    }
  },
  model: 'sleep-window',
  id: '1',
  route: 'delete'
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
        windowId: ':id'
      }
    }
  },
  model: 'sleep-window',
  id: '1'
});

// /sleep-windows/create
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
        event: {
          connect: {
            eventId: undefined
          }
        },
        startTime: undefined,
        stopTime: undefined,
        isSleep: undefined,
        note: null
      }
    }
  },
  model: 'sleep-window',
  route: 'create'
});

// /sleep-windows/update
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
      data: {},
      where: {
        windowId: ':id'
      }
    }
  },
  model: 'sleep-window',
  id: '1',
  route: 'update'
});

// /sleep-windows/delete
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
        windowId: ':id'
      }
    }
  },
  model: 'sleep-window',
  id: '1',
  route: 'delete'
});