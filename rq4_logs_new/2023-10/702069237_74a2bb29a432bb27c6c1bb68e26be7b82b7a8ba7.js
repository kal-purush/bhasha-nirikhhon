const router = require('express').Router();
const { itemSchema } = require('../schema');
const bodyParser = require('body-parser');

router.use(bodyParser.json());
router.use(bodyParser.urlencoded({ extended: true}));

router.post('/currCardItems', async (req, res) => {
    console.log(`Request current card items`, req.body);
    const arr = req.body;
    if (typeof arr !== 'object') {
        return res.json({status: 400, message: 'Invalid request body'});
    }
    const colorez = [];
    try {
        const matches = await Promise.all(arr.map(async item => {
            const {id, color, image, title, description} = item;
            const match = await itemSchema.findOne({_id: id});
            console.log(`koj`, match);
            if (!match) {
                throw new Error(`Item with id ${id} not found`);
            }
            const existingItem = colorez.find(i => i.id === id);
            if (existingItem) {
                existingItem.color.push(color);
                existingItem.count++;
            } else {
                colorez.push({id: id, color: [color], image: match.image, title: match.title, description: match.description, count: 1});
            }
            return match;
        }));
        console.log(`cwqtowe`, colorez)
        res.json({status: 200, count: matches.length, message: colorez});
    } catch (err) {
        console.error(err);
        res.json({status: 500, message: 'Error processing request'});
    }
});

module.exports = router;