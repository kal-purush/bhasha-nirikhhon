import { prisma } from "../src/server/db";

async function main() {
  const employmentId = "employment";
  const educationId = "education";
  const pltrId = "pltr";
  const uqId = "uq";
  const studyId = "study";

  const palantirPayload = {
    sectionId: employmentId,
    id: pltrId,
    title: "Palantir",
    subTitle: "2023 -",
    body: "Since February 2023 I have been working as a Forward Deployed Software Engineer with Palantir Technologies in Sydney Australia.",
  };

  const uqPayload = {
    sectionId: employmentId,
    id: uqId,
    title: "The University of Queensland",
    subTitle: "2021-2022",
    body: "I was a tutor at the University of Queensland. During my time I have tutored Mathematics for Calculus and Linear Algebra I and Mathematical Foundations I, taking classes of 30-50 students.",
  };

  const studyPayload = {
    sectionId: educationId,
    id: studyId,
    title: "The University of Queensland",
    subTitle: "2019-2022",
    body: "Bachelor of Computer Science and Bachelor of Mathematics, majoring in Machine Learning and Pure Mathematics respectively.",
  };

  await prisma.section.upsert({
    where: {
      id: employmentId,
    },
    create: {
      id: employmentId,
    },
    update: {},
  });

  await prisma.card.upsert({
    where: {
      id: pltrId,
    },
    create: palantirPayload,
    update: palantirPayload,
  });

  await prisma.card.upsert({
    where: {
      id: uqId,
    },
    create: uqPayload,
    update: uqPayload,
  });

  await prisma.section.upsert({
    where: {
      id: educationId,
    },
    create: {
      id: educationId,
    },
    update: {},
  });

  await prisma.card.upsert({
    where: {
      id: studyId,
    },
    create: studyPayload,
    update: studyPayload,
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