import { UserNotFoundError } from "@/core/errors/user-not-found";
import { FastifyReply, FastifyRequest } from "fastify";
import { z } from "zod";
import { makeEditProfileUseCase } from "./factories/make-edit-profile-use-case";

const editProfileSchema = z.object({
  first_name: z.string().optional(),
  last_name: z.string().optional(),
  phone_number: z.string().optional()
})

export class EditProfileController {
  handle = async (req: FastifyRequest, reply: FastifyReply) => {
    const data = editProfileSchema.parse(req.body);

    try {
      const sut = makeEditProfileUseCase()
      const updatedProfile = await sut.execute({
        id: req.user.sub,
        ...data
      });
    } catch (err) {
      if (err instanceof UserNotFoundError) {
        return reply.status(404).send(err.message)
      }

      return reply.status(400)
    }
  }
}