import { Prisma, PrismaClient } from "@prisma/client";

const prisma = new PrismaClient()

const seedUsers = async()=> {
  const alice = await prisma.user.upsert({
    where: { email: 'maria@email.com' },
    update: {},
    create: {
      email: 'maria@email.com',
      name: 'Maria',
      password: '$2b$10$hHYKQW.ioMR9Y/UKw7lNBu27FtiXQdSkzl9BEE7kp.jYl14OPcR0i' // senha: 123456
    },
  })
  const bob = await prisma.user.upsert({
    where: { email: 'bob@email.com' },
    update: {},
    create: {
      email: 'bob@email.com',
      name: 'Bob',
      password: '$2b$10$qp6RoAwT8eDaobf8AuoKTuXhYUWNWNa1dHmRWockCA.mOkZi9Bagu' // senha: 654321

    },
  })
}
seedUsers()
  .then(async () => {
    await prisma.$disconnect()
  })
  .catch(async (e) => {
    console.error(e)
    await prisma.$disconnect()
    process.exit(1)
  })