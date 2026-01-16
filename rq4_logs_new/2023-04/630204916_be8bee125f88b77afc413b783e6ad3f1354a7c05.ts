import { Request, Response } from 'express';
import { db } from '../../connection/connection';
import Travel from '../../schema/travels';


export const newTravel = async (req: Request, res: Response) => {
    try {
        const data: Travel = req.body;
        const docRef = await db.collection('travels').add(data);

        // Actualizar el historial de viajes del usuario correspondiente
        const userId = data.userId;
        const userRef = db.collection('users').doc(userId);
        const userSnapshot = await userRef.get();
        const userData = userSnapshot.data();
        if (userData) {
            const history = userData.travels || {};
            const travels = history.travels || [];
            travels.push(docRef.id);
            await userRef.update({ history: { ...history, travels } });
        }

        res.status(201).json({ id: docRef.id });
    } catch (error) {
        console.error('Error al crear el viaje', error);
        res.status(500).json({ message: 'Error al crear el viaje' });
    }
};