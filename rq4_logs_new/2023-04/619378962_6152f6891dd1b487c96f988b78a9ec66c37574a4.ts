import { Request, Response } from 'express';
import { GetUserByIdUseCase } from './getUserByIdUseCase';

class GetUserByIdController {
    constructor(
        private getUserByIdUseCase: GetUserByIdUseCase
    ) { }
    async handle(request: Request, response: Response): Promise<Response> {

        try {
            const { id } = request.params;
            const user = await this.getUserByIdUseCase.execute(id);
            return response.status(200).json(user);
        }
        catch (error) {
            return response.status(400).json({
                message: error.message || 'Unexpected error'
            });
        }
    }
}

export {
    GetUserByIdController
}