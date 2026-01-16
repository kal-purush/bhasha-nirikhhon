/* Para trabajar con el jwt */
const jwt = require('jsonwebtoken')

/* Middleware para trabajar con la validacion de rutas protegidas */
const verifyToken = (req, res, next) => {
    /* Obtenemos el token */
    const token = req.header('auth-token')
    /* Si el token no es valido, devolvemos un acceso denegado */
    if (!token) return res.status(401).json({ error: 'Acceso denegado' });
    /* Inciamos un try por si ocurre un error */
    try {
        /* Obtenemos la palabra secreta del archivo .env */
        const verified = jwt.verify(token, process.env.TOKEN_SECRET)
        /* Validamos si el token del body es verificado */
        req.user = verified
        /* continuamos */
        next()
    /* Cachamos un posible error */
    } catch (error) {
        /* Enviamos un mensaje informando que no es valido el token */
        res.status(400).json({error: 'Token no es válido'})
    }
}

/* Exportamos el middleware */
module.exports = verifyToken;