import { Request, Response } from 'express';
import { UpdateUserUseCase } from "./updateUserUseCase";

class UpdateUserController {
    constructor(
        private updateUserUseCase: UpdateUserUseCase
    ) { }
    async handle(request: Request, response: Response): Promise<Response> {
        const { id } = request.params;
        const { name, email, password } = request.body;

        try {
            const user = {
                name,
                email,
                password
            }
            await this.updateUserUseCase.execute(id, user);

            return response.status(201).send();
        }
        catch (error) {
            return response.status(400).json({
                message: error.message || 'Unexpected error'
            });
        }
    }
}

export {
    UpdateUserController
}