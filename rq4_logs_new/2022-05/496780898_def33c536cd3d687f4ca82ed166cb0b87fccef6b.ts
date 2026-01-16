const express = require('express');

import { MainMenu } from './repository/menu-repository';

export const router = express.Router();

// @route GET api/menu/main
// @description Get main menu
// @access Public
router.get('/main', (req, res) => {
    res.json(MainMenu);
});