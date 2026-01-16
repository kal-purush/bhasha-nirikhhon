const jwt = require('jsonwebtoken');

const verificarToken = (req, res, next) => {
    const token = req.headers['authorization'];

    if (!token) {
        return res.status(403).json({ message: 'Se requiere un token.' });
    }

    const tokenSinBearer = token.startsWith('Bearer ') ? token.slice(7) : token;

    jwt.verify(tokenSinBearer, 'tu_secreto', (error, decoded) => {
        if (error) {
            return res.status(401).json({ message: 'Token inválido.' });
        }
        req.userId = decoded.id; // Guardar el ID del usuario
        next();
    });
};

module.exports = verificarToken;