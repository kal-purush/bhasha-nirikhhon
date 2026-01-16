import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

async function main() {
  await prisma.adm.deleteMany();

  await prisma.adm.create({
    data: {
      name: "Bruno Augusto Lopes Fevereiro",
      area: "finance",
      email: "br.fevereiro@terra.com.br",
      rg: 558465584,
      password: "senha123",
    },
  });
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });