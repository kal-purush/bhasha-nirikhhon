import express, { json, NextFunction, Request, Response } from "express";
import dotenv from "dotenv";
import cors from "cors";
import {userRoute,parcelRoute,notificationRoute} from "./routes";
const app = express();


dotenv.config();
app.use(json());
app.use(cors());
app.use('/api/users',userRoute)
app.use('/api/parcels',parcelRoute)
app.use('/api/notifications',notificationRoute)
app.use(function (err: Error, req: Request, res: Response, next: NextFunction) {
  if (err.message)
    return res.status(500).send({ message: "Internal Server Error" });

  res.status(404).send({ message: err });
});

const PORT = process.env.PORT;

app.listen(PORT, () => {
    
    console.log(`Server is running on port:${PORT}`)


});