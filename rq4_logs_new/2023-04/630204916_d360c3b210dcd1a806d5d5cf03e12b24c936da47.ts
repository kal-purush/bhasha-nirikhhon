import { Request, Response } from 'express';
import { db } from '../../connection/connection';
import Chauffeur from '../../schema/chauffeur';

export const newChauffeur = async (req: Request, res: Response): Promise<void> => {
  try {
    const data: Chauffeur = req.body;
    const snapshot = await db.collection('chauffeur').where('email', '==', data.email).get();
    if (!snapshot.empty) {
      throw new Error('El correo electrónico ya está registrado');
    }

    const docRef = await db.collection('chauffeur').add(data);
    res.status(201).json({ id: docRef.id });
  } catch (error) {
    try {
      throw new Error(error.message);
    } catch (innerError) {
      console.error('Error al crear el chofer', innerError);
      res.status(400).json({ message: innerError.message });
    }
  }
};

export const updateChauffeur = async (req: Request, res: Response): Promise<void> => {
  try {
    const id: string = req.params.id;
    const data: Chauffeur = req.body;
    const docRef = await db.collection('chauffeur').doc(id).get();
    if (!docRef.exists) {
      throw new Error('No se encontró el chofer')
    }
    await db.collection('chauffeur').doc(id).update(data);
    res.status(200).json({ message: 'Chofer actualizado correctamente'})
  } catch (error) {
    console.error('Error al actualizar el usuario', error);
    res.status(400).json({ message: error.message })
  }
}