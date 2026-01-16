const ErrorResponse = require("../utils/errorResponse");
const asyncHandler = require("../middlewares/async");
const Course = require("../models/Course");
const { query } = require("express");

//@desc     Get all Courses
//@route    GET /api/v1/courses
//@route    GET /api/v1/bootcamps/:bootcampId/courses
//@access   Public
exports.getCourses = asyncHandler(async (req, res, next) => {
  let query;

  if (req.params.bootcampId) {
    query = Course.find({ bootcamp: req.params.bootcampId });
  } else {
    query = Course.find();
  }
  //Copy req.query
  //   const reqQuery = { ...req.query };

  //Fields to exclude
  //   const removeFields = ["select", "sort", "page", "limit"];

  //Loop over removeFields and delete the from reqQuery
  //   removeFields.forEach((param) => delete reqQuery[param]);

  //Create query string
  //   let queryStr = JSON.stringify(reqQuery);

  // Create operators ($gt,$gte, $lt, $lte and $in)
  //   queryStr = queryStr.replace(
  //     /\b(gt|gte|lt|lte|in)\b/g,
  //     (match) => `$${match}`
  //   );
  // Finging resources
  //   query = Bootcamp.find(JSON.parse(queryStr));

  // Select fields
  //   if (req.query.select) {
  //     const fields = req.query.select.split(",").join(" ");
  //     query = query.select(fields);
  //   }

  //   //Sort
  //   if (req.query.sort) {
  //     const sortBy = req.query.sort.split(",").join(" ");
  //     query = query.sort(sortBy);
  //   } else {
  //     query = query.sort("-createdAt");
  //   }
  //Pagination
  //   const page = parseInt(req.query.page, 10) || 1;
  //   const limit = parseInt(req.query.limit, 10) || 25;
  //   const startIndex = (page - 1) * limit;
  //   const endIndex = page * limit;
  //   const total = await Bootcamp.countDocuments();

  //   query = query.skip(startIndex).limit(limit);

  //Executing query
  const courses = await query;

  // Pagination
//   const pagination = {};

//   if (endIndex < total) {
//     const pageString = `page=${page}`;
//     let nextUrl = req.query.page
//       ? `${req.hostname}${req.originalUrl.replace(
//           pageString,
//           `page=${page + 1}`
//         )}`
//       : `${req.hostname}${req.originalUrl}&page=${page + 1}`;
//     pagination.next = {
//       page: page + 1,
//       limit,
//       url: nextUrl,
//     };
//   }

//   if (startIndex > 0) {
//     const pageString = `page=${page}`;
//     let prevUrl = req.query.page
//       ? `${req.hostname}${req.originalUrl.replace(
//           pageString,
//           `page=${page - 1}`
//         )}`
//       : `${req.hostname}${req.originalUrl}&page=${page - 1}}`;
//     pagination.prev = {
//       page: page - 1,
//       limit,
//       url: prevUrl,
//     };
//   }

  res.status(200).json({
    sucess: true,
    msg: "Showing all courses",
    count: courses.length,
    // pagination,
    data: courses,
  });
});

//@desc     Get a single bootcamp
//@route    GET /api/v1/bootcamps/:id
//@access   Public
exports.getBootcamp = asyncHandler(async (req, res, next) => {
  const bootcamp = await Bootcamp.findById(req.params.id);
  if (!bootcamp) {
    return next(
      new ErrorResponse(`Bootcamp not found with id of ${req.params.id}`, 404)
    );
  }
  res.status(200).json({
    sucess: true,
    msg: `Showing a bootcamp ${req.params.id}`,
    data: bootcamp,
  });
});

//@desc     Create new bootcamp
//@route    POST /api/v1/bootcamps
//@access   Private
exports.createBootcamp = asyncHandler(async (req, res, next) => {
  const bootcamp = await Bootcamp.create(req.body);
  res.status(201).json({
    sucess: true,
    msg: `Created a bootcamp`,
    data: bootcamp,
  });
});

//@desc     Update a bootcamp
//@route    PUT /api/v1/bootcamps/:id
//@access   Private
exports.updateBootcamps = asyncHandler(async (req, res, next) => {
  const bootcamp = await Bootcamp.findByIdAndUpdate(req.params.id, req.body, {
    new: true,
    runValidators: true,
  });
  if (!bootcamp) {
    return next(
      new ErrorResponse(`Bootcamp not found with id of ${req.params.id}`, 404)
    );
  }
  res.status(200).json({
    sucess: true,
    msg: `Updated bootcamp ${req.params.id}`,
    data: bootcamp,
  });
});

//@desc     Delete a bootcamp
//@route    DEL /api/v1/bootcamps/:id
//@access   Private
exports.deleteBootcamps = asyncHandler(async (req, res, next) => {
  const bootcamp = await Bootcamp.findByIdAndDelete(req.params.id);
  if (!bootcamp) {
    return next(
      new ErrorResponse(`Bootcamp not found with id of ${req.params.id}`, 404)
    );
  }
  res.status(200).json({
    sucess: true,
    msg: `Deleted bootcamp ${req.params.id}`,
    data: {},
  });
});

//@desc     Get bootcamps within a radius
//@route    GET /api/v1/bootcamps/radius/:zipcode/:distance
//@access   Private
exports.getCoursesInRadius = asyncHandler(async (req, res, next) => {
  const { zipcode, distance } = req.params;

  //Get lat/lbg from geocoder
  const loc = await geocoder.geocode(zipcode);
  const lat = loc[0].latitude;
  const lng = loc[0].longitude;

  /**
   * Claculate the raduis using radians
   * divide dist by the radius of the Earth
   * Earth Radius = 3,963mi / 6,378 km
   */
  const raduis = distance / 3963;

  const bootcamps = await Bootcamp.find({
    location: {
      $geoWithin: {
        $centerSphere: [[lng, lat], raduis],
      },
    },
  });

  // if (!bootcamps) {
  //   return next(
  //     new ErrorResponse(`No Bootcamps found within this Zipcode and location`, 404)
  //   );
  // }
  res.status(200).json({
    sucess: true,
    count: bootcamps.length,
    msg: `Found some bootcampswithin this Zipcode and location`,
    data: bootcamps,
  });
});