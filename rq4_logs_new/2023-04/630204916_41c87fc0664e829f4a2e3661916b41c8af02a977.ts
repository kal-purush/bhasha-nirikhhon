import { Request, Response } from 'express';
import { db } from '../../connection/connection';
import Chauffeur from '../../schema/chauffeur';

export const searchChauffeur = async (req: Request, res: Response): Promise<void> => {
    try {
        const id: string = req.params.id;
        const doc = await db.collection('chauffeur').doc(id).get();
        if (!doc.exists) {
            res.status(404).json({ message: 'Chofer no encontrado' });
        } else {
            const chauffeur = doc.data() as Chauffeur;
            res.json(chauffeur);
        }
    } catch (error) {
        console.error('Error al obtener el chofer', error);
        res.status(500).json({ message: 'Error al obtener el chofer' });
    }
}

export const allChauffeur = async (req: Request, res: Response): Promise<void> => {
    try {
        const chauffeurRef = db.collection('chauffeur');
        const chauffeurSnapshot = await chauffeurRef.get();
        const chauffeurs = [];
        chauffeurSnapshot.forEach((doc) => {
            const chauffeur = {
                id: doc.id,
                ...doc.data(),
            };
            chauffeurs.push(chauffeur);
        })
        res.json(chauffeurs)
    } catch (error) {
        console.error('Error al obtener los choferes', error)
        res.status(500).json({ message: 'Error al obtener los choferes' })
    }
}

export const searchChauffeurByPatent = async (req: Request, res: Response): Promise<void> => {
    try {
        const patent: string = req.params.patent;
        const doc = await db.collection('chauffeur').where("vehicle.patent", "==", patent).get();

        if (doc.empty) {
            res.status(404).json({ message: `No se encontró ningún chofer con la patente ${patent}` });
        }
        const chauffeurData = doc.docs[0].data() as Chauffeur;
        res.json(chauffeurData);
    } catch (error) {
        console.error('Error al obtener el chofer', error)
        res.status(500).json({ message: 'Error al obtener el chofer' })
    }
}