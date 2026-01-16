import { FastifyRequest, FastifyReply } from "fastify";
import { authenticateUser } from "../services/authService";

// Handle POST /login
export async function loginHandler(
  request: FastifyRequest,
  reply: FastifyReply
) {
  const { username, password } = request.body as {
    username: string;
    password: string;
  };

  try {
    const user = await authenticateUser(username, password);

    if (!user) {
      reply.status(401).send({ error: "Invalid username or password" });
      return;
    }

    reply.status(200).send({ message: "Login successful", user: { username } });
  } catch (error) {
    reply.status(500).send({ error: "Internal Server Error" });
  }
}