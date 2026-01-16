import { Request, Response } from 'express';
import { pool, router } from '../database/CarmineDB'

router.get('/getPets', async (req: Request, res: Response) => {
    try {
        const result = await pool.query('SELECT * FROM pets');
        res.status(200).json(result.rows);
    } catch (error) {
        console.error('Error fetching pets:', error);
        res.status(500).json({ message: 'Error fetching pets' });
    }
});

router.put('/updatePet', async (req: Request, res: Response) => {
    const { pet_id, pet_currency, pet_progress_bar } = req.body;
    try {
        const result = await pool.query(
            'UPDATE pets SET pet_currency = $1, pet_progress_bar = $2 WHERE pet_id = $3 RETURNING *',
            [pet_currency, pet_progress_bar, pet_id]
        );
        res.status(200).json(result.rows[0]);
    } catch (error) {
        console.error('Error updating pet:', error);
        res.status(500).json({ message: 'Error updating pet' });
    }
});

export default router;