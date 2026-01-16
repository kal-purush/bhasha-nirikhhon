import { getServerSession } from "next-auth";
import { authOptions } from "@/auth";
import prisma from "@/lib/db";
import { z } from "zod";

export async function PATCH(request: Request) {
  const session = await getServerSession(authOptions);

  if (!session) {
    return new Response(JSON.stringify({ message: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const body = await request.json();

    const assignSchema = z.object({
      ticketId: z.number().int(),
      userId: z.number().int(),
    });

    const parsedBody = assignSchema.safeParse(body);

    if (!parsedBody.success) {
      return new Response(JSON.stringify(parsedBody.error.errors), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    const { ticketId, userId } = parsedBody.data;

    const user = await prisma.person.findUnique({
      where: { id: userId },
      select: { role: true },
    });

    if (!user) {
      return new Response(
        JSON.stringify({
          message: "El usuario asignado debe tener rol de 'User'",
        }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    if (user.role !== "User") {
      return new Response(
        JSON.stringify({
          message: "El usuario asignado debe tener rol de 'User'",
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    await prisma.ticket.update({
      where: { id: ticketId },
      data: { userId: userId },
    });

    return new Response(
      JSON.stringify({ message: "Técnico asignado al ticket exitosamente" }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Error al asignar técnico al ticket" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}