import path from "path";
import fs from "fs/promises";
import { currentUser } from "@clerk/nextjs/server";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export async function POST(request: Request) {
  const session = await currentUser();

  if (session) {
    const body = await request.formData();

    const img = body.get("drone-img") as File;
    const serialNumber = body.get("serial-number") as string;
    const model = body.get("model") as string;
    const availability = body.get("availability") as string;
    try {
      await fs.readdir(path.join(process.cwd() + "/public", "/media"));
    } catch (error) {
      await fs.mkdir(path.join(process.cwd() + "/public", "/media"));
    }

    if (img && serialNumber && model && availability) {
      const fileName = `DRONE-${generateRandomString(15)}.${
        img.name.split(".")[img.name.split(".").length - 1]
      }`;

      await writeFile(Buffer.from(await img.arrayBuffer()), fileName);

      await prisma.drone.create({
        data: {
          serialNumber,
          model,
          availability: availability === "true" ? "AVAILABLE" : "BUSY",
          image: fileName,
        },
      });
      return Response.json({ done: "OK" }, { status: 201 });
    } else {
      return new Response("Bad Request", { status: 400 });
    }
  } else {
    return new Response("Unauthorized", { status: 401 });
  }
}

async function writeFile(fileContent: Buffer, fileName: string): Promise<void> {
  const filePath = path.join(process.cwd() + "/public", "/media", fileName);

  await fs.writeFile(filePath, fileContent);
}

function generateRandomString(length: number) {
  const characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const charactersLength = characters.length;

  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }

  return result;
}