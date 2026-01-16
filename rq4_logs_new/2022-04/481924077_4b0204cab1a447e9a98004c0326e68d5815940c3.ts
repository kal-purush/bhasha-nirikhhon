import cors from "cors";
import express, { Request, Response, NextFunction } from "express";
import router from "./routers/index.js";

const app = express();
app.use(express.json());
app.use(cors());
app.use(router);

app.use((error, req: Request, res: Response, next: NextFunction) => {
    console.log(error);
    if (error.response) {
        return res.sendStatus(error.response.status);
    }
    res.sendStatus(500);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log("Running on " + PORT);
});