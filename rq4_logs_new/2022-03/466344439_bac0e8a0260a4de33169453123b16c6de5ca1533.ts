import nextConnect from 'next-connect';
import { NextApiRequest, NextApiResponse } from "next";
import multer from 'multer';
import { PrismaClient } from '@prisma/client';
import fs from 'fs'
import { v4 as uuidv4 } from 'uuid';

const upload = multer({
    storage: multer.diskStorage({
        destination: `./public/uploads`,
        filename: (req: any, file: { originalname: any; }, cb: (arg0: null, arg1: any) => any) => cb(null, `${uuidv4()}${file.originalname}`),
    }),
});

const uploadMiddleware = upload.array('file');

const apiRoute = nextConnect({
    onNoMatch(req: NextApiRequest, res: NextApiResponse) {
        res.status(405).json({ error: `Method '${req.method}' Not Allowed` });
    },
});
apiRoute.use(uploadMiddleware);

const prisma = new PrismaClient()
apiRoute.post(async (req: NextApiRequest & { files: any }, res: NextApiResponse) => {
    const { title, description, tags, userId } = req.body
    const formatedTags = JSON.parse(tags)
    console.log('files', req.files)

    try {
        const prisRes = await prisma.animation.create({ data: { description, title, path: req.files[0].filename, userId: Number(userId) } })
        if (prisRes) {
            const arrTag = formatedTags.map((tg: Number) => { return { animationId: prisRes.id, tagId: tg } })
            const addTag = await prisma.tagOnAnimation.createMany({ data: arrTag })
        }

    } catch (error) {
        console.log('error', error)
        return res.status(500).json({ data: 'Something went wrong' });

    }
    return res.status(201).json({ data: 'Animation created successfully' });

});

apiRoute.delete(async (req: NextApiRequest, res: NextApiResponse) => {
    const { id } = req.query
    try {
        const prisRes = await prisma.animation.delete({ where: { id: Number(id) } })
        if (prisRes) {
            fs.unlink(`./public/uploads/${prisRes.path}`, err => {
                if (err) console.log(err)
            });

        }

    } catch (error) {
        console.log('error', error)
        return res.status(500).json({ data: 'Something went wrong' });

    }
    return res.status(201).json({ data: 'Animation deleted successfully' });

})

export default apiRoute;

export const config = {
    api: {
        bodyParser: false,
    },
};