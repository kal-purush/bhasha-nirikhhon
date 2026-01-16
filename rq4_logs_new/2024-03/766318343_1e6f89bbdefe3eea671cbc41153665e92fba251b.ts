import { PrismaClient } from "@prisma/client";

import articles from "../../src/data/articles.json";
import { slugify } from "../../src/utils";

const prisma = new PrismaClient();

const isDev = process.env.NODE_ENV === "development";

async function main() {
  await seedArticles();
}

async function seedArticles() {
  if (!isDev) throw new Error("Seeding is only allowed in development mode");

  await prisma.article.deleteMany();
  await prisma.$executeRawUnsafe(
    "DELETE FROM SQLITE_SEQUENCE WHERE name=$1;",
    "Article"
  );

  for (const article of articles) {
    await prisma.article.create({
      data: {
        title: article.title,
        slug: slugify(article.title),
        excerpt: article.excerpt,
        content: article.content,
        image: article.image,
        publishedAt: new Date(article.publish_date),
      },
    });
  }
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async () => {
    await prisma.$disconnect();
    process.exit(1);
  });