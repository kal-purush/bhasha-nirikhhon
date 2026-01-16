import { RequestHandler } from 'express';
import Joi from 'joi';
import { RegisterRequestDTO } from '../database/dto/AuthenticationDTO';

export class AuthenticationValidation {

    private passwordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+[\]{};':"\\|,.<>?])(?!.+(0123|1234|2345|3456|4567|5678|6789)).+$/;

    private registerSchema = Joi.object({
        name: Joi.string().required(),
        email: Joi.string().email().required(),
        password: Joi.string().pattern(this.passwordRegex).required(),
        userType: Joi.string().valid('student', 'teacher').required(),
        weigth: Joi.string().optional(),
        height: Joi.string().optional(),
        workoutDays: Joi.string().optional(),
        any_disease: Joi.string().optional()
    });

    private loginSchema = Joi.object({
        email: Joi.string().email().required(),
        password: Joi.string().required()
    });

    public validateRegister: RequestHandler = async (req, res, next) => {
        
        let registerRequest = <RegisterRequestDTO>req.body;

        this.registerSchema.validateAsync(registerRequest).then(() => {
            req.body = registerRequest;
            next();
        }).catch((err) => {
            res.status(400).send({ message: err.details });
        });
    };

    public validateLogin: RequestHandler = async (req, res, next) => {
        
        let loginRequest = req.body;

        this.loginSchema.validateAsync(loginRequest).then(() => {
            req.body = loginRequest;
            next();
        }).catch((err) => {
            res.status(400).send({ message: err.details });
        });
    };
};
