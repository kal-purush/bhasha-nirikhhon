import { PrismaClient } from "@prisma/client";
import faker from "faker";

const prisma = new PrismaClient();

async function main() {
  console.log(`Start seeding ...`);

  const usersData = [...new Array(10)].map(() => {
    return {
      githubEmail: faker.internet.email(),
      githubId: faker.datatype.number(),
      bio: faker.lorem.sentences(3),
      githubAvatarUrl: faker.internet.avatar(),
      githubName: faker.name.findName(),
      githubUsername: faker.internet.userName(),
      location: faker.address.country(),
      githubUrl: "",

      posts: {
        createMany: {
          data: {
            description: faker.lorem.sentences(3),
            title: faker.lorem.sentences(2),
            slug: faker.lorem.slug(),
            published: true,
          },
        },
      },
    };
  });

  for (const data of usersData) {
    await prisma.user.create({
      data,
    });
  }

  console.log("Seeding finished.");
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });