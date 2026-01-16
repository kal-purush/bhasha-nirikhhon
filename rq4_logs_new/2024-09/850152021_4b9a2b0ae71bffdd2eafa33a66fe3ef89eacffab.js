const express = require('express');
const quoteUseCase = require('../usecases/quote.usecase');

const router = express.Router();

router.post('/create', async (req,res) => {
    try {
        const { clientId, carId, mechanicId, items } = req.body;
        const quote = await quoteUseCase.create({ clientId, carId, mechanicId, items });
        res.json({
            success: true,
            data: { quote }, 
        })
    } catch (error) {
        res.status(error.estatus || 500);
        res.json({
            success: false,
            error: error.message,
        });
    }
});

module.exports = router;