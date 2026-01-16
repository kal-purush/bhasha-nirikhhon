import { Router } from "express";

import { Game } from "../../../models/Game";

import { auth } from "../../middleware/auth";

import ee from "../../../controllers/EventEmmiter"

const router = Router();

// @route GET api/gamerecord
// @access public
router.get('/', (req, res) => {
    Game.find()
        .sort({ date: -1 })
        .then((gamerecord) => {
            res.json(gamerecord);
        })
})

// @route POST api/gamerecord
// @access public
router.post('/', (req, res) => {
    new Game({
        title: req.body.title,
        url: req.body.url,
        logo: req.body.logo
    }).save()
        .then((gamerecord) => {
            ee.emit("gamerecord.created", gamerecord)
            res.json(gamerecord);
        })

})

router.post('/:id', (req, res) => {
    Game.findOneAndUpdate({ name: req.params.id }, { ...req.body })
        .then((gamerecord) => {
            res.json({ gamerecord });
        })
        .catch((err) => {
            res.status(404).json({ success: false, err })
        });

})

// @route DELETE api/gamerecord/:id
// @access public
router.delete('/:id', (req, res) => {
    Game.findById(req.params.id)
        .then((gamerecord) => {
            gamerecord.remove()
                .then(() => {
                    res.json({ success: true });
                })
        })
        .catch((err) => {
            res.status(404).json({ success: false })
        });
})


export default router;