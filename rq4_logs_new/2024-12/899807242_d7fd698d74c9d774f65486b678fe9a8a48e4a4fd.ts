import { authOptions } from "@/auth";
import { getServerSession } from "next-auth";
import prisma from "@/lib/db";

export async function GET() {
  const session = await getServerSession(authOptions);

  if (!session) {
    return new Response(JSON.stringify({ message: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    // Consulta tickets con un usuario asignado
    const tickets = await prisma.ticket.findMany({
      where: {
        userId: { not: null }, // Solo tickets asignados
      },
      select: {
        id: true,
        title: true,
        userId: true, // Incluye el ID del usuario asignado
      },
    });

    return new Response(JSON.stringify(tickets), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error("Error al obtener tickets asignados:", error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener los tickets asignados" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}