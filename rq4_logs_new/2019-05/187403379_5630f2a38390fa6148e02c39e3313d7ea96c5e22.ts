import express from 'express';
import {Logger} from '@mazemasterjs/logger';
import Config from '@mazemasterjs/shared-library/Config';

export const probesRouter = express.Router();

const log: Logger = Logger.getInstance();
const config: Config = Config.getInstance();

const RES_LIVE_TRUE: object = {probeType: 'liveness', status: 'alive'};
const RES_LIVE_WAIT: object = {probeType: 'liveness', status: 'awaiting-ready'};
const RES_READY_TRUE: object = {probeType: 'readiness', status: 'ready'};
const RES_READY_WAIT: object = {probeType: 'readiness', status: 'not-ready'};

/**
 * Liveness probe for container/cloud hosted service monitoring
 */
probesRouter.get('/live', (req, res) => {
    log.trace(__filename, 'Route -> [' + req.url + ']', 'Handling request.');
    if (config.READY_TO_ROCK) {
        log.trace(__filename, 'Route -> [' + req.url + ']', JSON.stringify(RES_LIVE_TRUE));
        res.status(200).json(RES_LIVE_TRUE);
    } else {
        log.force(__filename, 'Route -> [' + req.url + ']', JSON.stringify(RES_LIVE_WAIT));
        res.status(500).json(RES_LIVE_WAIT);
    }
    log.trace(__filename, 'Route -> [' + req.url + ']', 'Response complete.');
});

/**
 * Readiness probe for container/cloud hosted service monitoring
 */
probesRouter.get('/ready', (req, res) => {
    log.trace(__filename, 'Route -> [' + req.url + ']', 'Handling request.');
    if (config.READY_TO_ROCK) {
        log.trace(__filename, 'Route -> [' + req.url + ']', JSON.stringify(RES_READY_TRUE));
        res.status(200).json(RES_READY_TRUE);
    } else {
        log.force(__filename, 'Route -> [' + req.url + ']', JSON.stringify(RES_READY_WAIT));
        res.status(500).json(RES_READY_WAIT);
    }
    log.trace(__filename, 'Route -> [' + req.url + ']', 'Response complete.');
});

export default probesRouter;