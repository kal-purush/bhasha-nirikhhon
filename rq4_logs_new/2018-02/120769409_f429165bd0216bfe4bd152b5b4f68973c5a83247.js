import {existingRootId, existingTrunkId, rootIdIsNotTrunkId} from "../../const/validations";
import {insertRoot} from "../../service/root/rootCommands";

const run = require('../../util/run');
const router = require('express').Router();

module.exports = router;

router.post('/api/link',
    [
        existingTrunkId,
        existingRootId,
        rootIdIsNotTrunkId
    ],
    run(insertLink)
);