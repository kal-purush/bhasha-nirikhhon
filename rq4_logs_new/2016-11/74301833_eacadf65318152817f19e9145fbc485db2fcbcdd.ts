/**
 * Created by JJax on 19.11.2016.
 */
import * as express from "express";
import credentials from "../credentials";
import ProjectService from  "../service/projectService";

export default (addon) => {
    const router = express.Router();
    const httpClient = addon.httpClient(credentials);
    const projectService = new ProjectService();

    router.get('/project', (req, res) => {
        projectService.getProjectCards(httpClient, (cards) => {
            res.setHeader("Content-Type", "application/json");
            res.send(cards);
        });
    });

    return router;
};