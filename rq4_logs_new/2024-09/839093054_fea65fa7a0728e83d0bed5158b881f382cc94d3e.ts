import { z } from "zod";
import { db } from "../../db";

import {
  createTRPCRouter,
  protectedProcedure,
  publicProcedure,
} from "~/server/api/trpc";

export const patientRecordRouter = createTRPCRouter({

    createPatientRecord: publicProcedure.input(z.object({
        name:z.string(),
        b_date:z.date().optional(),
        r_date:z.date().optional(),
        dx:z.string(),
        notes:z.string(),
    })).mutation(({input}) => {
        const patientRecord = db.record.create({
            data:{
                name: input.name,
                birth_date: input.b_date || new Date(), 
                register_date: input.r_date || new Date(),
                dx: input.dx,
                notes: input.notes
            }
        });
        return   patientRecord;
    }),

    getPatientRecords: publicProcedure.query(async () => {
        const patientRecords = await db.record.findMany();
        return patientRecords;
    }),

});