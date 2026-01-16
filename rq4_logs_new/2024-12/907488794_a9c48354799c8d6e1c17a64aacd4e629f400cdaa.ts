import { UserResponse } from "../types/userTypes";
import { authService, loginSchema } from "../services/authServices";
import { FastifyInstance } from "fastify";

export default async function authController(server: FastifyInstance) {
  server.post("/login", async (request, reply) => {
    const body = loginSchema.parse(request.body);  // A validação será feita aqui

    const user = await authService.loginUser(body);
    const token = authService.generateToken(user, server);

    const userResponse: UserResponse = {
      id: user.id,
      name: user.name,
      email: user.email,
      subscriptionPlan: user.subscriptionPlan,
      role: user.role,
    };

    return reply.send({ token, user: userResponse });
  });
}