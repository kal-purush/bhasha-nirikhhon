import express from "express";
import { Db } from "mongodb";
import Album from "../../models/Album";

export default function AlbumRouter(db: Db) {
    const router = express.Router();

    router.post('/album', async (req, res) => {
        const { user_id, title } = req.body as { title: string, user_id: string };
        const album = new Album({ title, creator: user_id });

        let result;
        try {
            result = await db.collection('albums').insertOne(album);
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error creating album', message });
        }

        if (result.acknowledged) return res.status(201).send(album);
        return res.status(500).send({ error: 'error creating album' });
    });

    router.get('/album', async (req, res) => {
        const { album_id } = req.query as { album_id: string };

        let album;
        try {
            album = await db.collection('albums').findOne(
                { album_id },
                { projection: { _id: 0 } }
            );
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error creating album', message });
        }

        if (album) return res.status(200).send(album);
        return res.status(404).send({ error: 'album not found' });
    });

    router.get('/albums', async (req, res) => {
        const { query } = req.query as { query: string };

        let albums;
        try {
            albums = await db.collection('albums').find(
                { title: { $regex: query, $options: 'i' } },
                { projection: { _id: 0 } }
            ).toArray();
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error creating album', message });
        }

        if (albums) return res.status(200).send(albums);
        return res.status(404).send({ error: 'no albums found' });
    });

    return router;
}