import { authOptions } from "@/auth";
import { getServerSession } from "next-auth";
import prisma from "@/lib/db";
import { Prisma, Status } from "@prisma/client";

export async function GET(request: Request) {
  const session = await getServerSession(authOptions);

  if (!session) {
    return new Response(JSON.stringify({ message: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  const { searchParams } = new URL(request.url);
  const route = searchParams.get("route");

  switch (route) {
    case "summary":
      return getSummary();
    case "resolution-rate":
      return getResolutionRate();
    case "monthly-summary":
      return getMonthlySummary();
    case "by-category":
      return getByCategory();
    case "technician-performance":
      return getTechnicianPerformance();
    case "company-summary":
      return getCompanySummary();
    default:
      return new Response(JSON.stringify({ message: "Invalid route" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
  }
}

async function getSummary() {
  try {
    const totalTickets = await prisma.ticket.count();
    const pendingTickets = await prisma.ticket.count({ where: { status: Status.Open } });
    const completedTickets = await prisma.ticket.count({ where: { status: Status.Closed } });

    return new Response(JSON.stringify({
      totalTickets,
      pendingTickets,
      completedTickets,
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener el resumen de tickets" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

async function getResolutionRate() {
  try {
    const totalTickets = await prisma.ticket.count();
    const completedTickets = await prisma.ticket.count({ where: { status: Status.Closed } });
    const resolutionRate = (completedTickets / totalTickets) * 100;

    return new Response(JSON.stringify({ resolutionRate }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener la tasa de resolución" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

async function getMonthlySummary() {
  try {
    const monthlyData = await prisma.ticket.groupBy({
      by: ['createdAt'],
      _count: {
        _all: true,
      },
      orderBy: {
        createdAt: 'asc',
      },
    });

    return new Response(JSON.stringify(monthlyData), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener el resumen mensual de tickets" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

async function getByCategory() {
  try {
    const ticketsByCategory = await prisma.ticket.groupBy({
      by: ['type'],
      _count: {
        _all: true,
      },
    });

    return new Response(JSON.stringify(ticketsByCategory), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener el desglose por categoría" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

async function getTechnicianPerformance() {
  try {
    const ticketsByTechnician = await prisma.ticket.groupBy({
      by: ['userId'],
      _count: {
        _all: true,
      },
    });

    return new Response(JSON.stringify(ticketsByTechnician), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener el rendimiento de los técnicos" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

async function getCompanySummary() {
  try {
    const ticketsByCompany = await prisma.ticket.groupBy({
      by: ['clientId'],
      _count: {
        _all: true,
      },
    });

    return new Response(JSON.stringify(ticketsByCompany), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(
      JSON.stringify({ message: "Hubo un error al obtener el resumen por empresa" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}