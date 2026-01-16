/* eslint-disable @typescript-eslint/no-require-imports */
const { PrismaClient } = require("@prisma/client");

const prisma = new PrismaClient();

async function seed() {
  await prisma.user.createMany({
    data: [
      {
        id: "e1668919-8c2d-4e6c-9b59-72b6c83cb8f8",
        name: "John Doe",
        email: "john@email.com",
        password: "hashed-password",
      },
      {
        id: "7375567e-2f02-4fe5-8fe5-1233c3986c9f",
        name: "Jane Doe",
        email: "jane@email.com",
        password: "hashed-password",
      },
    ],
  });
}

seed()
  .then(() => {
    console.log("\n Database Seeded.");
  })
  .catch((e) => {
    console.error("Error on seeding database: ", e);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });