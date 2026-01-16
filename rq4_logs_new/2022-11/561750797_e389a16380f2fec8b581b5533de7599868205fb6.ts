import { PrismaClient } from '@prisma/client';

// initialize Prisma Client
const prisma = new PrismaClient();

async function main() {
  const customer1 = await prisma.customer.upsert({
    where: { id: 1 },
    update: {},
    create: {
      name: 'John Wick',
      address: 'The Continental',
      active: true,
    },
  });

  const customer2 = await prisma.customer.upsert({
    where: { id: 2 },
    update: {},
    create: {
      name: 'Bruce Wayne',
      address: 'Gotham Wayne Manor',
      active: true,
    },
  });

  const customer3 = await prisma.customer.upsert({
    where: { id: 3 },
    update: {},
    create: {
      name: 'Valerie',
      address: 'Night City',
    },
  });

  console.log({ customer1, customer2, customer3 });
}

// execute the main function
main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    // close Prisma Client at the end
    await prisma.$disconnect();
  });