import { body } from "express-validator";

export const handleLogin = () => (
    [body('email').notEmpty().isEmail().withMessage('Invalid email format!'),
    body('password').notEmpty().isStrongPassword().withMessage('Invalid password format!')]
)