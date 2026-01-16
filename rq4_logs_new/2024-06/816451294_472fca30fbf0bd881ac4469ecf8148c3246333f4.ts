import { getServerSession } from "next-auth/next"
import * as z from "zod"

import { authOptions } from "@/lib/auth"
import { db } from "@/lib/db"
import { eventPatchSchema } from "@/lib/validations/event"

// Define schema for route context
const routeContextSchema = z.object({
  params: z.object({
    eventId: z.string(),
  }),
})

export async function GET(req: Request, context: z.infer<typeof routeContextSchema>) {
  try {
    // Validate route params
    const { params } = routeContextSchema.parse(context)

    // Fetch the event
    const event = await db.event.findUnique({
      where: {
        id: params.eventId,
      },
      select: {
        id: true,
        title: true,
        content: true,
        startDate: true,
        endDate: true,
        createdAt: true,
      },
    })

    if (!event) {
      return new Response("Event not found", { status: 404 })
    }

    return new Response(JSON.stringify(event))
  } catch (error) {
    if (error instanceof z.ZodError) {
      return new Response(JSON.stringify(error.issues), { status: 422 })
    }

    return new Response(null, { status: 500 })
  }
}

export async function PATCH(req: Request, context: z.infer<typeof routeContextSchema>) {
  try {
    // Validate route params
    const { params } = routeContextSchema.parse(context)

    // Validate the user session
    const session = await getServerSession(authOptions)
    if (!session) {
      return new Response("Unauthorized", { status: 403 })
    }

    // Check if the user has access to this event
    const hasAccess = await verifyCurrentUserHasAccessToEvent(params.eventId, session.user.id)
    if (!hasAccess) {
      return new Response(null, { status: 403 })
    }

    // Get and validate the request body
    const json = await req.json()
    const body = eventPatchSchema.parse(json)

    // Update the event
    const updatedEvent = await db.event.update({
      where: {
        id: params.eventId,
      },
      data: {
        title: body.title,
        content: body.content,
        startDate: body.startDate ? new Date(body.startDate) : undefined,
        endDate: body.endDate ? new Date(body.endDate) : undefined,
      },
      select: {
        id: true,
      },
    })

    return new Response(JSON.stringify(updatedEvent))
  } catch (error) {
    if (error instanceof z.ZodError) {
      return new Response(JSON.stringify(error.issues), { status: 422 })
    }

    return new Response(null, { status: 500 })
  }
}

export async function DELETE(req: Request, context: z.infer<typeof routeContextSchema>) {
  try {
    // Validate the route params
    const { params } = routeContextSchema.parse(context)

    // Validate the user session
    const session = await getServerSession(authOptions)
    if (!session) {
      return new Response("Unauthorized", { status: 403 })
    }

    // Check if the user has access to this event
    const hasAccess = await verifyCurrentUserHasAccessToEvent(params.eventId, session.user.id)
    if (!hasAccess) {
      return new Response(null, { status: 403 })
    }

    // Delete the event
    await db.event.delete({
      where: {
        id: params.eventId,
      },
    })

    return new Response(null, { status: 204 })
  } catch (error) {
    if (error instanceof z.ZodError) {
      return new Response(JSON.stringify(error.issues), { status: 422 })
    }

    return new Response(null, { status: 500 })
  }
}

async function verifyCurrentUserHasAccessToEvent(eventId: string, userId: string) {
  const count = await db.event.count({
    where: {
      id: eventId,
      authorId: userId,
    },
  })

  return count > 0
}