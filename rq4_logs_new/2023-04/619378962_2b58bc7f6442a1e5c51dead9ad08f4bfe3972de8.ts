import { Request, Response } from 'express';
import { GetAllUsersUseCase } from './getAllUsersUseCase';

export class GetAllUsersController {
    constructor(
        private getAllUsersUseCase: GetAllUsersUseCase
    ) { }
    async handle(request: Request, response: Response): Promise<Response> {

        try {
            const users = await this.getAllUsersUseCase.execute()

            return response.status(201).json(users);
        }
        catch (error) {
            return response.status(400).json({
                message: error.message || 'Unexpected error'
            });
        }
    }
}