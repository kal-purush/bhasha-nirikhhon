import express from "express";
import fs from 'fs';
import { Db } from "mongodb";
import Album from "../../models/Album";

const outputDir = process.env.OUTPUT_DIR || './';

export default function AlbumRouter(db: Db) {
    const router = express.Router();

    router.post('/album', async (req, res) => {
        const { user_id, title } = req.body as { title: string, user_id: string };
        const album = new Album({ title, creator: user_id });

        try {
            await new Promise<void>((resolve, reject) => {
                if (fs.existsSync(`${outputDir}/${album.album_id}`)) return reject(new Error('album already exists or you managed to generate a v4 collision lol'));
                fs.mkdir(`${outputDir}/${album.album_id}/thumb_cache`, { recursive: true }, err => {
                    if (err) return reject(err);
                    resolve();
                });
            });
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error creating album', message });
        }

        // don't do this synchronously with the previous operation because we don't want to make
        // a db entry if we failed to create the directory
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
        const page = +req.query.page! || 0;

        let album;
        try {
            album = await db.collection('albums').findOne(
                { album_id },
                { projection: { _id: 0 } }
            );
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error fetching album', message });
        }

        if (!album) return res.status(404).send({ error: 'album not found' });

        let imageEntries;
        try {
            imageEntries = await db.collection('images')
                .find({ album_id }, { projection: { _id: 0, album_id: 0 } })
                .sort({ uploaded_at: -1, image_id: 1 })
                .skip(page * 20)
                .limit(20)
                .toArray();
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error fetching album', message });
        }

        if (!imageEntries?.length) return res.status(200).send({ album, images: [] });

        const images = await Promise.all(imageEntries.map(async image => ({
            ...image,
            // if the base64 data is null we will just use the default thumbnail
            base64: await getThumbnail(album_id, image.image_id)
        })));

        res.status(200).send({ album, images, next_page: imageEntries.length === 20 ? page + 1 : null });
    });

    router.get('/albums', async (req, res) => {
        const { query } = req.query as { query: string };
        const page = +req.query.page! || 0;

        let albums;
        try {
            albums = await db.collection('albums')
                .find({ title: { $regex: query, $options: 'i' } }, { projection: { _id: 0 } })
                .sort({ title: 1, album_id: 1 })
                .skip(page * 10)
                .limit(10)
                .toArray();
        } catch (error) {
            let message = 'an unknown error has occurred';
            if (error instanceof Error) message = error.message;
            return res.status(500).send({ error: 'error fetching albums', message });
        }

        if (albums) return res.status(200).send(albums);
        return res.status(404).send({ error: 'no albums found' });
    });

    return router;
}

export function getThumbnail(album_id: string, image_id: string): Promise<string | null> {
    return new Promise(resolve => {
        fs.readFile(`${outputDir}/${album_id}/thumb_cache/${image_id}.thumb`, (err, data) => {
            if (err) return resolve(null);
            resolve(Buffer.from(data).toString('base64'));
        });
    });
}