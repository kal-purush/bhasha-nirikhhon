/**
 * Created by JJax on 19.11.2016.
 */
/**
 * Created by JJax on 19.11.2016.
 */
import * as express from "express";
import credentials from "../credentials";
import JqlService from "../service/jqlService";

export default (addon) => {
    const router = express.Router();
    const httpClient = addon.httpClient(credentials);
    const jqlService = new JqlService();

    router.get('/', (req, res) => {
        console.log(req.query.request);
        var toReturn = jqlService.doRequest(req.query.request, httpClient, (jqlRes) => {
            res.send(jqlRes);
        });
    });

    return router;
};