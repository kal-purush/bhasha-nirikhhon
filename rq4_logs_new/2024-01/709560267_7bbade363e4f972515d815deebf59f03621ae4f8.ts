import { and, eq, inArray } from "drizzle-orm";
import { db } from "./db"
import { applicationQuestions, applicationResponses, applications } from "~/server/db/schema"
import { FieldType } from "~/server/db/types";
import * as crypto from "crypto"

export const createApplications = async (userIds: string[], cycleId: string) => {
    await db
        .insert(applications)
        .values(userIds.map(id => ({ userId: id, cycleId: cycleId })));

    return db
        .select()
        .from(applications)
        .where(and(
            inArray(applications.userId, userIds),
            eq(applications.cycleId, cycleId)
        ));
}

export const fillApplications = async (applicationIds: string[], cycleId: string) => {
    const questions = await db
        .select()
        .from(applicationQuestions)
        .where(eq(applicationQuestions.cycleId, cycleId)
        );

    return Promise.all(applicationIds.map(async (id) => {
        const responses: { questionId: string, value: string, applicationId: string }[] = [];
        for (const q of questions) {
            const response: typeof responses[number] = { questionId: q.id, applicationId: id, value: "" };
            if (q.type === FieldType.STRING) {
                q.minLength = q.minLength ?? 0;
                q.maxLength = q.maxLength ?? q.minLength + 40;
                const length = Math.floor((Math.random() * (q.maxLength - q.minLength))) + (q.minLength)
                response.value = crypto.randomBytes(Math.ceil(length / 2)).toString('hex');
            } else if (q.type === FieldType.BOOLEAN) {
                response.value = ["false", "true"][Math.floor(Math.random() * 2)]!
            } else if (q.type === FieldType.CHECKBOX) {
                response.value = q.options?.reduce((accum: string[], o: string) => {
                    if (Math.random() < 0.5) {
                        accum.push(o);
                    }
                    return accum;
                }, []).join(",,,") ?? ""
            } else if (q.type === FieldType.MULTIPLE_CHOICE || q.type === FieldType.DROPDOWN) {
                if (q.options) {
                    response.value = q.options[Math.floor(Math.random() * q.options.length)]!
                }
            }
            responses.push(response);
        }

        return db.insert(applicationResponses).values(responses);
    }))
};

export const submitApplications = (applicationIds: string[]) => {
    return db.update(applications).set({ submitted: true }).where(inArray(applications.id, applicationIds));
}

