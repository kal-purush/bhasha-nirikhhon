const pool = require('../db');

const UsuarioRol = async (req, res) => {
    try {
        console.log("Datos recibidos en /UsuarioRol:", req.body);
        const { idUsuario, idRol } = req.body;

        const result = await pool.query(
            `UPDATE public."USUARIO"
             SET "ID_rol" = $1
             WHERE "ID_usuario" = $2
             RETURNING *;`,
            [idRol, idUsuario]
        );

        // Validar que se haya actualizado algún usuario
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Usuario no encontrado' });
        }

        const usuarioActualizado = result.rows[0];
        res.status(200).json({
            message: 'Rol del usuario actualizado correctamente',
            usuario: usuarioActualizado
        });
    } catch (error) {
        console.error('Error en /UsuarioRol:', error);
        res.status(500).json({
            error: 'Error interno del servidor',
            detalle: error.message
        });
    }
};



module.exports = {
    UsuarioRol
};