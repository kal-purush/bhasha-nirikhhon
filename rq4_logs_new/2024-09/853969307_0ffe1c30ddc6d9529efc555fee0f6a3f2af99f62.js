import { asyncHandler } from "../utils/asyncHandler.js";
import { apiError } from "../utils/apiError.js";
import { apiResponse } from "../utils/apiResponse.js";
import { Tweet } from "../models/tweets.model.js";

const createTweet = asyncHandler(async (req, res) => {
  const { content } = req.body;

  if (!content) {
    throw new apiError(400, "Content is required.");
  }

  try {
    const tweet = await Tweet.create({
      content,
      owner: req?.user._id,
    });

    return res
      .status(201)
      .json(new apiResponse(200, tweet, "Tweet created successfully."));
  } catch (error) {
    throw new apiError(
      400,
      error?.message || "Somthing went wrong while create tweets."
    );
  }
});

const getAllTweets = asyncHandler(async (req, res) => {
  try {
    const { userId } = req.params;

    const tweets = await Tweet.find({
      owner: userId,
    });

    if (tweets.length <= 0) {
      throw new apiError(400, "Tweets not fetched");
    }

    return res
      .status(200)
      .json(
        new apiResponse(200, tweets, "All tweets are fetched successfully.")
      );
  } catch (error) {
    throw new apiError(
      400,
      error?.message || "Tweets not fetched somthing error."
    );
  }
});

const updateTweet = asyncHandler(async (req, res) => {
  const { content } = req.body;
  const { tweetId } = req.params;

  if (!content) {
    throw new apiError(400, "Content is required.");
  }

  try {
    const tweet = await Tweet.findByIdAndUpdate(
      { _id: tweetId },
      {
        $set: { content },
      },
      { new: true }
    );

    if (!tweet) {
      throw new apiError(400, "Tweet not updated");
    }

    return res
      .status(200)
      .json(new apiResponse(200, tweet, "Tweet updated successfully. "));
  } catch (error) {
    throw new apiError(
      400,
      error?.message || "Somthing went worng while tweets updation."
    );
  }
});

const deleteTweet = asyncHandler(async (req, res) => {
  try {
    const { tweetId } = req.params;

    const tweet = await Tweet.findByIdAndDelete({
      _id: tweetId,
    });

    if (!tweet) {
      throw new apiError(400, "Tweet not deleted");
    }

    return res
      .status(200)
      .json(new apiResponse(200, "Tweet deleted successfully"));
  } catch (error) {
    throw new apiError(400, error?.message);
  }
});

export { createTweet, getAllTweets, updateTweet, deleteTweet };