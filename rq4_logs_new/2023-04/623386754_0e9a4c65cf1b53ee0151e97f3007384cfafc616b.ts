// EXPRESS
import { RequestHandler } from "express";
// UTILITY FUNCTIONS
import findDocumentByIdAndModel from "../utilities/controllers/findDocumentByIdAndModel";
// MODELS
import { Review, Product } from "../models";
// HTTP CODES
import { StatusCodes } from "http-status-codes";
// INTERFACES
import { ReviewSchemaInterface } from "../utilities/interfaces";

const createReview: RequestHandler = async (req, res) => {
  // GET PRODUCT ID
  const { id: productId } = req.params;
  // REVIEW KEYS FROM THE CLIENT TO CREATE A NEW REVIEW
  const {
    title,
    comment,
    rating,
  }: Omit<ReviewSchemaInterface, "user | product"> = req.body;
  // USER ID
  const userId = req.user?._id;
  // FIND THE PRODUCT
  const product = await findDocumentByIdAndModel({
    id: productId,
    MyModel: Product,
  });
  // CREATE THE REVIEW
  const review = await Review.create({
    title,
    comment,
    rating,
    user: userId,
    product: productId,
  });

  res
    .status(StatusCodes.CREATED)
    .json({ msg: "review created", product, review });
};

const deleteReview: RequestHandler = async (req, res) => {
  // GET REVIEW ID
  const { id: reviewId } = req.params;
  // FIND REVIEW FROM DB
  const review = await findDocumentByIdAndModel({
    id: reviewId,
    MyModel: Review,
  });
  // DELETE REVIEW
  await review.deleteOne();

  res.status(StatusCodes.OK).json({ msg: "review deleted" });
};

const getAllReviews: RequestHandler = async (req, res) => {
  // GET PRODUCT ID
  const { id: productId } = req.params;
  // GET REVIEW PAGE
  const { reviewPage }: { reviewPage: number } = req.body;
  // FIND THE PRODUCT
  const product = await findDocumentByIdAndModel({
    id: productId,
    MyModel: Product,
  });
  // FIND THE REVIEWS BY PRODUCT ID
  // SKIP NECESSARY PART AND LIMIT IT TO 10
  const limit = 10;
  const skip = limit * reviewPage ? reviewPage - 1 : 1;
  const reviews = await Review.find({ product: productId })
    .skip(skip)
    .limit(limit);

  res
    .status(StatusCodes.OK)
    .json({ msg: "reviews recieved", product, reviews });
};