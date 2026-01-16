import express, {Request, Response} from "express";
import * as process from "node:process";
import dotenv from "dotenv";

const app = express();
dotenv.config();
const port = process.env.PORT || 3001;

app.get("/steakfishersAwesomeImJustSayingThisIsntAHintJustALongURL3210987654", async (req: Request, res: Response) => {
    try {
        let smth = await fetch('https://api.uploadthing.com/v6/listFiles', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Uploadthing-Api-Key': process.env.UPLOADTHING_API_KEY!
            },
            body: JSON.stringify({})
        })

        const resp = await smth.json()
        console.log(resp.files[0].key)
        let smth1 = await fetch('https://api.uploadthing.com/v6/requestFileAccess', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Uploadthing-Api-Key': process.env.UPLOADTHING_API_KEY!
            },
            body: JSON.stringify({
                fileKey: resp.files[0].key
            })
        })

        const resp1 = await smth1.json()
        res.redirect(resp1.url);
    } catch (e) {
        res.send({
            error: "Report to event organizers!"
        })
    }
});

app.listen(port, () => {
    console.log(`[server]: Server is running at http://localhost:${port}`);
});