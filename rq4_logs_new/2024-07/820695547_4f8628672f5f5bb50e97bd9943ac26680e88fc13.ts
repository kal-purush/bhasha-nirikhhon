import { z } from "zod";
import { db } from "../../db";

import {
  createTRPCRouter,
  protectedProcedure,
  publicProcedure,
} from "~/server/api/trpc";

export const collaboratorsRouter = createTRPCRouter({
    createCollaborator: protectedProcedure.input(z.object({
        name:z.string(),
        career:z.string(),
        semester:z.string(),
    })).mutation(({input,ctx}) => {
        const collaborator = db.collaborator.create({
            data:{
                name: input.name,
                career: input.career,
                semester: input.semester,
                userId: ctx.session.user.id,
            }
        });
        return collaborator;
    }),

});