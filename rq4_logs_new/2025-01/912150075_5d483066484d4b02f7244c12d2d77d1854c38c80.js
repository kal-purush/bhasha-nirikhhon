import express from "express";
import { createReadStream, statSync } from "fs";
import { dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();

app.get("/", (req, res) => {
    res.send("Hello World");
});

app.get("/video", (req, res) => {
    const file = `${__dirname}/public/Navratri.mp4`;

    try {
        const stat = statSync(file);
        const fileSize = stat.size;
        const range = req.headers.range;

        if (!range) {
            res.status(400).send("Requires Range Header");
            return;
        }

        const chunkSize = 10 ** 6; // 1MB chunk size
        const start = Number(range.replace(/\D/g, ""));
        const end = Math.min(start + chunkSize - 1, fileSize - 1);
        const contentLength = end - start + 1;

        const headers = {
            "Content-Range": `bytes ${start}-${end}/${fileSize}`,
            "Accept-Ranges": "bytes",
            "Content-Length": contentLength,
            "Content-Type": "video/mp4",
        };

        res.writeHead(206, headers);

        const fileStream = createReadStream(file, { start, end });
        fileStream.pipe(res);
    } catch (err) {
        console.error("Error handling video stream:", err);
        res.status(500).send("Internal Server Error");
    }
});

// Static routes for additional videos
app.get("/video/Christmas", (req, res) => {
    const file = `${__dirname}/public/CHRISTMAS.mp4`;
    res.sendFile(file, (err) => {
        if (err) {
            console.error("Error sending file:", err);
            res.status(500).send("Internal Server Error");
        }
    });
});

app.get("/video/one", (req, res) => {
    const file = `${__dirname}/public/001.mp4`;
    res.sendFile(file);
});

app.get("/video/two", (req, res) => {
    const file = `${__dirname}/public/VIDEO.mp4`;
    res.sendFile(file);
});

app.get("/video/five", (req, res) => {
    const file = `${__dirname}/public/video5.mp4`;
    res.sendFile(file);
});

app.get("/video/three", (req, res) => {
    const file = `${__dirname}/public/003.mp4`;
    res.sendFile(file);
});

app.get("/video/video1", (req, res) => {
    const file = `${__dirname}/public/video1.mp4`;
    res.sendFile(file);
});

app.get("/video/002", (req, res) => {
    const file = `${__dirname}/public/002.mp4`;
    res.sendFile(file);
});

app.get("/video/videoBG", (req, res) => {
    const file = `${__dirname}/public/VideoBg.mp4`;
    res.sendFile(file);
});

app.listen(3000, () => {
    console.log("Server is running on port 3000");
});