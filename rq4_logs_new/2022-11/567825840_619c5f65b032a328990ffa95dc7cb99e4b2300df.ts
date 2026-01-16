import { NextFunction, Request, Response } from 'express';
import { RobotRepository } from '../repository/robot.repository.js';
import { RobotController } from '../controllers/robotController.js';

jest.mock('../repository/robot.mongo.repository');

describe('Given RobotController', () => {
    RobotRepository.prototype.getAll = jest.fn().mockResolvedValue(['robot']);
    RobotRepository.prototype.get = jest
        .fn()
        .mockResolvedValue(['63767bb8d09c41ef88a55efd']);
    const repository = new RobotRepository();

    const RobotController = new RobotController(repository);
    const req: Partial<Request> = {};
    const resp: Partial<Response> = {
        json: jest.fn(),
    };
    const next: NextFunction = jest.fn();
    test('Then ... getAll', async () => {
        await RobotController.getAll(req as Request, resp as Response, next);
        expect(resp.json).toHaveBeenCalledWith({
            robot: ['robot'],
        });
    });
    test('Then if we use get(), it should have been called', async () => {
        await RobotController.get(req as Request, resp as Response, next);
        expect(resp.json).toHaveBeenCalled();
    });
});