const catchAsync = require('../utils/catchAsync');
const pick = require('../utils/pick');
const transportationService  = require('../services/transportation');


exports.getAllTransportation = catchAsync(async (req, res) => {
    const filter = pick(req.query, ['name', 'type']);
    const options = pick(req.query, ['limit', 'page']);
    const result = await transportationService.queryTransportation(filter, options);
    res.send(result);
});