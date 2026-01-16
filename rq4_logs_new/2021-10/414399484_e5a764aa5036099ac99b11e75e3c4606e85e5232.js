const path = require('path');
const { route } = require('../apiRoutes/notesRoutes');
const router = require('express').Router();

//html routes
router.get('/', (_, res) => {
    res.sendFile(path.join(__dirname, '../../public/index.html'));
});

router.get('/notes', (_, res) => {
    res.sendFile(path.join(__dirname, '../../public/notes.html'));
});

router.get('*', (_, res) => {
    res.sendFile(path.join(__dirname, '../../public/index.html'));
});

module.exports = router;