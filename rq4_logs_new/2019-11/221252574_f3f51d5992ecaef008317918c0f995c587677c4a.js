const Calendar = require('../models/Calendar')

// module.exports.All = async function (req, res) {
// //     const count = await Calendar.count()
// //     const allpage = Math.ceil(count/req.body.limit)
// //     const all = await Calendar.find({room: req.params.id}).limit(req.body.limit).skip((req.body.page-1)*10)
// //
// //
// //     try {
// //         if (all) {
// //             res.status(200).json({count, allpage, all})
// //         }
// //     }
// //     catch (err) {
// //         //обработать ошибку
// //         res.status(500).json({
// //             success: false,
// //             message: err.message ? err.message : err
// //         })
// //     }
// //
// // }

module.exports.Allid = async function (req, res) {
    const count = await Calendar.count({room: req.params.id})
    const allpage = Math.ceil(count/req.body.limit)
    const all = await Calendar.find({room: req.params.id}).limit(req.body.limit).skip((req.body.page-1)*10)

    try {
        if (all) {
            res.status(200).json({count, allpage, all})
        }
    }
    catch (err) {
        //обработать ошибку
        res.status(500).json({
            success: false,
            message: err.message ? err.message : err
        })
    }

}

module.exports.Booking = async function (req, res) {
    const opp = await Calendar.find({$and:[{"date" : {$gte : req.body.datestart, $lte : req.body.dateend}},
                                           {"avail": true},
                                           {"room": req.params.id}]})

    try {
    if (opp.length !== 0) {
        await Calendar.updateMany(
            {$and:[{"date" : {$gte : req.body.datestart, $lte : req.body.dateend}},
                   {"avail": true},
                   {"room": req.params.id}]},              // критерий фильтрации
            { $set: {avail: false}})                    // параметр обновления
            .then(res.status(200).json({booking: true}))
    } else {
        // если нет возможности забронировать
        res.status(410).json({
            code: 'error',
            message: 'В данный период комната забронированна'
        })
    }
    }
    catch (err) {
        //обработать ошибку
        res.status(500).json({
            success: false,
            message: err.message ? err.message : err
        })

    }

}

module.exports.unBooking = async function (req, res) {
    // const opp = await Calendar.find({$and:[{"date" : {$gte : req.body.datestart, $lte : req.body.dateend}},
    //         {"avail": true},
    //         {"room": req.params.id}]})

    try {
            await Calendar.updateMany(
                {$and:[{"date" : {$gte : req.body.datestart, $lte : req.body.dateend}},
                       {"room": req.params.id}]},              // критерий фильтрации
                { $set: {avail: true}})                    // параметр обновления
                .then(res.status(200).json({unbooking: true}))
    }
    catch (err) {
        //запрос не выполнился
        res.status(500).json({
            success: false,
            message: err.message ? err.message : err
        })

    }

}


