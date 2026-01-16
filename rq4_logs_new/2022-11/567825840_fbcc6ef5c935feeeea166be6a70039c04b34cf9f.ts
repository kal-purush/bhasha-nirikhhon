import { NextFunction, Request, Response } from 'express';
import { RobotTypes } from '../entities/robot.Types.js';
import { CustomError, HTTPError } from '../interfaces/error.js';

import { RobotRepository } from '../repository/robot.repository.js';
import { RobotController } from './robotController.js';

jest.mock('../repository/robot.repository.js');

const mockData: Array<RobotTypes> = [
    {
        id: 'string',
        robotName: 'string',
        velocity: 2,
        resistent: 4,
        creationDate: 'string',
        img: 'string',
    },
    {
        id: 'string',
        robotName: 'string',
        velocity: 1,
        resistent: 2,
        creationDate: 'string',
        img: 'string',
    },
];

describe('Given RobotController', () => {
    describe('When we instantiate getAll()', () => {
        RobotRepository.prototype.getAll = jest
            .fn()
            .mockResolvedValue(mockData);
        RobotRepository.prototype.get = jest
            .fn()
            .mockResolvedValue(mockData[0]);
        RobotRepository.prototype.post = jest.fn().mockResolvedValue(mockData);
        RobotRepository.prototype.patch = jest.fn().mockResolvedValue(mockData);
        RobotRepository.prototype.delete = jest
            .fn()
            .mockResolvedValue(mockData);
        const repository = new RobotRepository();
        const robotController = new RobotController(repository);

        const req: Partial<Request> = {};
        const res: Partial<Response> = {
            json: jest.fn(),
        };
        const next: NextFunction = jest.fn();
        test('Then getAll should have been called', async () => {
            await robotController.getAll(req as Request, res as Response, next);
            expect(res.json).toHaveBeenCalledWith({ data: mockData });
        });

        test('Then get should have been called', async () => {
            req.params = { id: '0' };
            await robotController.get(req as Request, res as Response, next);
            expect(res.json).toHaveBeenCalledWith({ data: mockData[0] });
        });

        test('Then post should have been called', async () => {
            await robotController.post(req as Request, res as Response, next);
            expect(res.json).toHaveBeenCalledWith({ data: mockData });
        });

        test('Then patch should have been called', async () => {
            await robotController.patch(req as Request, res as Response, next);
            expect(res.json).toHaveBeenCalledWith({ data: mockData });
        });

        test('Then delete should have been called', async () => {
            await robotController.delete(req as Request, res as Response, next);
            expect(res.json).toHaveBeenCalledWith({ data: mockData });
        });
    });
});

describe('Given', () => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    let error: CustomError;
    beforeEach(() => {
        error = new HTTPError(418, 'Tea Pot', 'Ja, ja, ja');
    });
    RobotRepository.prototype.getAll = jest.fn().mockRejectedValue(['Robot']);
    RobotRepository.prototype.get = jest.fn().mockResolvedValue('Robot');
    RobotRepository.prototype.post = jest.fn().mockRejectedValue(['Robot']);
    RobotRepository.prototype.patch = jest.fn().mockRejectedValue(['Robot']);
    RobotRepository.prototype.delete = jest.fn().mockRejectedValue(['Robot']);
    const repository = new RobotRepository();
    const robotController = new RobotController(repository);

    const req: Partial<Request> = {};
    const res: Partial<Response> = {
        json: jest.fn(),
    };
    const next: NextFunction = jest.fn();
    test('Then when call the method GetAll cant get a response, it should return an error', async () => {
        (repository.getAll as jest.Mock).mockRejectedValue('Error');
        await robotController.getAll(
            req as Request,
            res as Response,
            next as NextFunction
        );
        expect(error).toBeInstanceOf(Error);
        expect(error).toBeInstanceOf(HTTPError);
    });
    test('Then when call the method Get cant get a response, it should return an error', async () => {
        (repository.get as jest.Mock).mockRejectedValue('Error');
        await robotController.get(
            req as Request,
            res as Response,
            next as NextFunction
        );
        expect(error).toBeInstanceOf(Error);
        expect(error).toBeInstanceOf(HTTPError);
    });
    test('Then when call the method post cant get a response, it should return an error', async () => {
        (repository.post as jest.Mock).mockRejectedValue('Error');
        await robotController.post(
            req as Request,
            res as Response,
            next as NextFunction
        );
        expect(error).toBeInstanceOf(Error);
        expect(error).toBeInstanceOf(HTTPError);
    });

    test('Then when call the method post cant get a response, it should return an error', async () => {
        (repository.patch as jest.Mock).mockRejectedValue('Error');
        await robotController.patch(
            req as Request,
            res as Response,
            next as NextFunction
        );
        expect(error).toBeInstanceOf(Error);
        expect(error).toBeInstanceOf(HTTPError);
    });
    test('Then when call the method post cant get a response, it should return an error', async () => {
        (repository.delete as jest.Mock).mockRejectedValue('Error');
        await robotController.delete(
            req as Request,
            res as Response,
            next as NextFunction
        );
        expect(error).toBeInstanceOf(Error);
        expect(error).toBeInstanceOf(HTTPError);
    });
});