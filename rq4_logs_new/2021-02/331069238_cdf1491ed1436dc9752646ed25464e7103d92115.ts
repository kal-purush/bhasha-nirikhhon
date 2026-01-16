const jwt = require('jsonwebtoken');

module.exports = function(req:any, res:any, next:any) {
    // Leer el token del header
    const stringToken = req.headers.authorization;
   
    const token = stringToken.split(" ");

    
    // Revisar si  hay token
    if(!token) {
        return res.status(401).json({msg: 'No hay Token, permiso no válido'})
    }

    // validar el token
    try {
        const cifrado = jwt.verify(token[1], process.env.TOKEN_SECRET);
        req.body.email = cifrado.email;
        next();
    } catch (error) {
        res.status(401).json({msg: 'Token no válido'});
    }
}