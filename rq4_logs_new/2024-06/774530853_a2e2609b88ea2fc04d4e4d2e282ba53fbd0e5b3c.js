import { pool } from "../db/dbconnect.js"

const suscribirseController = async (req, res) => {
    let email = req.body.email
    try {
        const query = 'INSERT INTO newsletter (email) VALUES (?)';
        let resultado = await pool.query(query, [email])
        res.setHeader("Content-Type", "application/json")
        res.status(200).json({message: `El correo ${email} fue cargado exitosamente al newsletter`})

    } catch (error) {
        res.setHeader("Content-Type", "application/json")
        return res.status(500).json("Error inesperado en el servidor al querer grabar email para newsletter")         
    }
}

export {suscribirseController}