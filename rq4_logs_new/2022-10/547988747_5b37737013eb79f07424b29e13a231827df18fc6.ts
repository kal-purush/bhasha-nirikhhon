import { Request, Response, Router } from "express";

export default function AnnouncementRoutes(router:Router){

    router.route("/").get()

    return router;
}