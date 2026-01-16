import { jest, afterEach } from '@jest/globals';
import { prisma } from '../prisma/client.js';

const prismaSpy = {
  users: {
    all: jest.spyOn(prisma.user, 'findMany'),
    ':id': jest.spyOn(prisma.user, 'findUnique'),
    search: jest.spyOn(prisma.user, 'findMany'),
    create: jest.spyOn(prisma.user, 'create'),
    update: jest.spyOn(prisma.user, 'update'),
    delete: jest.spyOn(prisma.user, 'delete')
  },
  events: {
    all: jest.spyOn(prisma.event, 'findMany'),
    ':id': jest.spyOn(prisma.event, 'findUnique'),
    search: jest.spyOn(prisma.event, 'findMany'),
    create: jest.spyOn(prisma.event, 'create'),
    update: jest.spyOn(prisma.event, 'update'),
    delete: jest.spyOn(prisma.event, 'delete')
  },
  plans: {
    all: jest.spyOn(prisma.plan, 'findMany'),
    ':id': jest.spyOn(prisma.plan, 'findUnique'),
    search: jest.spyOn(prisma.plan, 'findMany'),
    create: jest.spyOn(prisma.plan, 'create'),
    update: jest.spyOn(prisma.plan, 'update'),
    delete: jest.spyOn(prisma.plan, 'delete')
  },
  reminders: {
    all: jest.spyOn(prisma.reminder, 'findMany'),
    ':id': jest.spyOn(prisma.reminder, 'findUnique'),
    search: jest.spyOn(prisma.reminder, 'findMany'),
    create: jest.spyOn(prisma.reminder, 'create'),
    update: jest.spyOn(prisma.reminder, 'update'),
    delete: jest.spyOn(prisma.reminder, 'delete')
  },
  babies: {
    all: jest.spyOn(prisma.baby, 'findMany'),
    ':id': jest.spyOn(prisma.baby, 'findUnique'),
    search: jest.spyOn(prisma.baby, 'findMany'),
    create: jest.spyOn(prisma.baby, 'create'),
    update: jest.spyOn(prisma.baby, 'update'),
    delete: jest.spyOn(prisma.baby, 'delete')
  }
};

afterEach(() => {
  for (const controller in prismaSpy) {
    for (const route in prismaSpy[controller]) {
      prismaSpy[controller][route].mockReset();
    }
  }
});

export default prismaSpy;