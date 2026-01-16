import { z } from "zod";
import { db } from "../../db";

import {
    adminProcedure,
  createTRPCRouter,
  protectedProcedure,
  publicProcedure,
} from "~/server/api/trpc";

export const collaboratorRequestRouter = createTRPCRouter({

    createCollaboratorRequest: protectedProcedure.input(z.object({
        name:z.string(),
        career:z.string(),
        semester:z.string(),
        photoUrl:z.string(),
    })).mutation(({input,ctx}) => {
        const collaboratorRequest = db.collaboratorRequest.create({
            data:{
                name: input.name,
                career: input.career,
                semester: input.semester,
                userEmail: ctx.session.user.email,
                photoUrl: input.photoUrl
            }
        });
        return collaboratorRequest;
    }),

    getCollaboratorRequests: adminProcedure.query(async () => {
        const collaboratorRequests = await db.collaboratorRequest.findMany();
        return collaboratorRequests;
    }),

    deleteCollaboratorRequest: adminProcedure.input(z.object({
        id: z.string(),
    })).mutation(async ({ input }) => {
        const deletedRequest = await db.collaboratorRequest.delete({
            where: {
                id: input.id,
            },
        });
        return deletedRequest;
    }),

});