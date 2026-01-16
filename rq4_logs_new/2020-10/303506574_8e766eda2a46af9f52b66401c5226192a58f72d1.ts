import {Request, Response} from 'express';
import {getRepository} from 'typeorm';
import Orphanege from '../models/Orphanage';

export default {
    async create (request: Request, response: Response) {
        const {name, latitude, longitude, about, instructions, opening_hours, open_on_weekends} = request.body;

        const orphanagesRepository = getRepository(Orphanege);

        const orphanage = orphanagesRepository.create({name, latitude, longitude, about, instructions, opening_hours, open_on_weekends});

        await orphanagesRepository.save(orphanage); // salva no banco

        return response.status(201).json(orphanage); // 201 - algo foi criado
    }
}