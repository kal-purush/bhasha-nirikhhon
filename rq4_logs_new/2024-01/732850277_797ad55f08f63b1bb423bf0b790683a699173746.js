import { pool } from '../config/database.js';

const addUserLike = async (req, res) => {
    try {
        const { userEmail, productId } = req.body;
        const results = await pool.query(`
            INSERT INTO UserLikes (userEmail, productId)
            VALUES ($1, $2)
            RETURNING *`,
            [userEmail, productId]
            )
        res.status(201).json(results.rows[0]);
    } catch (error) {
        res.status(409).json({ error: error.message })
    }
}

const getUserLikes = async (req, res) => {
    try {
        const { userEmail } = req.params;
        const results = await pool.query('SELECT * FROM UserLikes WHERE userEmail = $1 ORDER BY productId ASC', [userEmail]);
        res.status(200).json(results.rows);
    } catch (error) {
        res.status(500).json({ error: error.message })
    }
}

const deleteUserLike = async (req, res) => {
    try {
        const { userEmail, productId } = req.params;
        await pool.query('DELETE FROM UserLikes WHERE userEmail = $1 AND productId = $2', [userEmail, productId]);
        res.status(200).json({ message: 'Like removed successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
}

export default {
    addUserLike,
    getUserLikes,
    deleteUserLike
}





