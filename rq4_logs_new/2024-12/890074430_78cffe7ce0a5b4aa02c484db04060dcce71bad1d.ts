import { randomUUID } from "node:crypto";
import { execSync } from "node:child_process";
import type { Environment } from "vitest";
import "dotenv/config";
import { PrismaClient } from "@prisma/client";
// postgresql://docker:docker@localhost:5434/apisolid?schema=public
// bcdedit /set hypervisorlaunchtype auto
// bcdedit /set hypervisorlaunchtype off

const prisma = new PrismaClient();

function generateDatabaseURL(schema: string) {
  if (!process.env.DATABASE_URL) {
    throw new Error("Please provide a DATABASE_URL environment variable.");
  }

  const url = new URL(process.env.DATABASE_URL);

  url.searchParams.set("schema", schema);

  return url.toString();
}

export default <Environment>{
  name: "prisma",
  transformMode: "web",
  async setup() {
    const schema = randomUUID();

    const databaseURL = generateDatabaseURL(schema);

    process.env.DATABASE_URL = databaseURL;

    // deploy ao inves de "dev" pq deploy apenas executa as migrations, enquanto que dev compara as existentes migrations e depois atualiza
    execSync("npx prisma migrate deploy");

    return {
      async teardown() {
        await prisma.$executeRawUnsafe(
          `DROP SCHEMA IF EXISTS "${schema}" CASCADE`,
        );

        await prisma.$disconnect();
      },
    };
  },
};