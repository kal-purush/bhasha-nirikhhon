import { prisma } from "@/lib/prisma";
import { memberJson } from "./seed-data/member-data";

async function main() {
  await prisma.member.deleteMany();
  await prisma.rugbyPayment.deleteMany();

  const CURRENT_YEAR = new Date().getFullYear();

  memberJson.map(async (member) => {
    const memberCreated = await prisma.member.create({
      data: {
        email: member.email,
        name: member.name,
        nickname: member.nickName,
        cpf: BigInt(member.cpf),
        rg: member.rg,
        educationInstituition: member.educationInstituition,
        ra: member.ra,
        course: member.course,
        yearOfGraduation: member.yearOfGraduation,
        phoneNumber: BigInt(member.phoneNumber),
        dateOfBirth: member.yearOfBirth,
        yearOfJoinOnRugbyMaua: member.yearOfJoinOnRugbyMaua,
        instagram: member.instagram,
        lawsLink: member.lawsLink,
        readyLink: member.readyLink,
        team: member.team,
        uniformSize: member.uniformSize,
        uniformNumber: member.uniformNumber ?? null,
      },
    });

    await prisma.rugbyPayment.create({
      data: {
        memberId: memberCreated.id,
        memberName: memberCreated.name,
        memberNickname: memberCreated.nickname,
        isPaying: memberCreated.yearOfGraduation >= CURRENT_YEAR,
      },
    });
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