const TourDate = require('./../models/tourDateModel');
const catchAsync = require('./../utils/catchAsync');

//api/v1/memes/:id/startDates -> post
exports.createTourDate = catchAsync(async (req, res, next) => {
  const doc = await TourDate.create(req.body);

  res.status(201).json({
    status: 'success',
    data: {
      data: doc
    }
  });
});