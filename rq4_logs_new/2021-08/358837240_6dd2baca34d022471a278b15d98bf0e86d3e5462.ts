import { seedDepartments } from './seeding-function/seedDepartments';
import { seedMembers } from './seeding-function/seedMembers';
import { seedParents } from './seeding-function/seedParents';
import { PrismaClient } from '@prisma/client';
import { seedDiscipleships } from './seeding-function/seedDiscipleships';

export const prisma = new PrismaClient();

async function seed() {
  await seedDepartments();
  await seedParents();
  await seedMembers();
  await seedDiscipleships();
  await seedMembers();
}

seed()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });