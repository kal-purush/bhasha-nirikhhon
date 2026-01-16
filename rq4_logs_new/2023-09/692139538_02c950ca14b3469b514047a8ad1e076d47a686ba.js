const router = require('express').Router();
const accessoryService = require('../services/accessoryService');

router.get('/create', (req, res) => {
    res.render('accessory/create');
});

router.post('/create', async (req, res) => {
const body = req.body;

await accessoryService.create(body);
   res.redirect('/');
})

router.get('/attach/:id', (req, res) => {
    res.render('accessory/attach');
});
module.exports = router;