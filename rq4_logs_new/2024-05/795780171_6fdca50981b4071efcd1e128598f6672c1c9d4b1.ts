import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

async function main() {
  await prisma.member.deleteMany();

  await prisma.member.createMany({
    data: [
      {
        email: "joao@example.com",
        name: "João Silva",
        nickName: "joaosilva",
        cpf: 12345678900,
        rg: 987654321,
        educationInstituition: "Universidade Federal",
        ra: 123456,
        course: "Direito",
        yearOfGraduation: 2023,
        phoneNumber: 5511999999999,
        yearOfBirth: 1995,
        instagram: "joaosilva",
        lawsLink: "https://example.com/laws",
        readyLink: "https://example.com/ready",
        team: "Team A",
        shirtSize: "M",
        shirtNumber: 10,
      },
    ],
  });
}

main()
  .then(async () => {
    console.log("Mock user created successfully");
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error("Error creating mock user:", e);
    await prisma.$disconnect();
    process.exit(1);
  });