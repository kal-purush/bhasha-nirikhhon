import { z } from "zod";

const GardianSchema = z.object({
    fathersName: z.string().trim().optional(),
    fathersNumber: z.string().trim().optional()
});



const StudentsInfoZODSchema = z.object({
    body: z.object({
        id: z.string(),
        name: z.string().trim().min(1, 'name is required here').max(20, 'name cannot exceed 20 characters'),
        password: z.string().optional(),
        adress: z.string().optional(),
        contactnumber: z.string().trim().optional(),
        country: z.string().trim().optional(),
        gender: z.enum(["male", "female"], {
            errorMap: () => ({ message: "The gender field must be only male and female" })
        }),
        gardian: GardianSchema.optional()
    })
});


export default StudentsInfoZODSchema