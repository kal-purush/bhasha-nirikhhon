import express from 'express';
import jwt from 'jsonwebtoken';

const SECRET = process.env.MY_TOKEN_SECRET;

const decodeToken = express.Router();

decodeToken.use(async (req, res, next) => {
    const token = req.token;

    if (!token) {
        return res.status(401).json({ msg: 'Token not provided.' });
    }

    try {
        // Verifica el token y lo decodifica
        const decoded = jwt.verify(token, SECRET);
        console.log('Decoded Token:', decoded);  // Verifica la decodificación

        req.user = decoded;  // Asigna la información del usuario a `req.user`
        next();
    } catch (err) {
        console.error('JWT Verification Error:', err);
        return res.status(400).json({ msg: 'Token error', error: err.message });
    }
});

export default decodeToken;