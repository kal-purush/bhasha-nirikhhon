import { Request, Response } from "express";
import { PrismaUserRepository } from "../../infra/repository/user-repository/prisma-user-repository";
import { AuthenticateUserUseCase } from "../../usecases/authenticate/authenticate-usar.usecase";

export class AuthController {
    private userGateway = new PrismaUserRepository();
    private authenticateUseCase = new AuthenticateUserUseCase(this.userGateway);

    /**
 * @swagger
 * /login:
 *   post:
 *     summary: Realiza o login de um usuário.
 *     description: Autentica o usuário com base no nome de usuário e senha e retorna um token JWT.
 *     tags:
 *       - Login
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               username:
 *                 type: string   
 *               password:
 *                 type: string
 *     responses:
 *       200:
 *         description: Login bem-sucedido, retorna um token JWT.
 *       401:
 *         description: Credenciais inválidas.
 *       500:
 *         description: Erro interno no servidor.
 */

    async login(req: Request, res: Response): Promise<void> {
        const { username, password } = req.body;

        try {
            const token = await this.authenticateUseCase.execute({ username, password });

            if(!token) {
                res.status(401).json({ error: "Credenciais inválidas" })
                return;
            }

            res.status(200).json({ token })
        } catch (error) {
            if (error instanceof Error){
                res.status(400).json({ error: error.message});
            } else {
                res.status(500).json({ error: "Erro interno no servidor." })
            }
        }
    }
}