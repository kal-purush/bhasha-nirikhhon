// HTTP CODES
import { StatusCodes } from "http-status-codes";
// EXPRESS
import { ErrorRequestHandler } from "express";

interface CustomErrorInterface {
  msg: string;
  statusCode: number;
}

const errorHandler: ErrorRequestHandler = (err, req, res, next) => {
  const customError: CustomErrorInterface = {
    msg: err.message || "something went wrong",
    statusCode: err.statusCode || StatusCodes.INTERNAL_SERVER_ERROR,
  };

  // DUPLICATE ERROR HANDLE
  if (err.code === 11000) {
    customError.msg = "product is already in our system";
    customError.statusCode = StatusCodes.CONFLICT;
  }

  // CAST ERROR HANDLE
  if (err.name === "CastError") {
    customError.msg = "product id type is wrong";
    customError.statusCode = StatusCodes.BAD_REQUEST;
  }

  // VALIDATION ERROR
  if (err.name === "ValidationError") {
    const errorArray: string[] = Object.keys(err.errors).map(
      (key) => err.errors[key].message
    );
    customError.msg = errorArray.toString();
    customError.statusCode = StatusCodes.UNPROCESSABLE_ENTITY;
  }
  res.status(customError.statusCode).json({ msg: customError.msg });
};

export default errorHandler;