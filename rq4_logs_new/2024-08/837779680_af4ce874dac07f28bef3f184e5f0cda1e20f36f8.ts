import { Router } from 'express';
import {
  createNewReview,
  getReviewById,
  deleteReviewById,
  updateReviewById,
} from '../controllers/reviewsController';
import { methodNotAllowed } from '../controllers/suspicionController';

import validateJoiRequest from '../middlewares/validateJoiRequest';
import { createReviewValidation,
  reviewIdValidation,updateReviewValidation
} from '../validators/reviewFieldsValidation';
import authMiddleware from '../middlewares/authMiddleware';

const reviewRouter = Router();

reviewRouter.route('/')
  .post(
    authMiddleware,
    validateJoiRequest({ bodySchema: createReviewValidation }),
    createNewReview,
  );

reviewRouter.route('/:id')
  .get(
    authMiddleware,
    validateJoiRequest({ paramsSchema: reviewIdValidation }),
    getReviewById,
  )
  .put(
    authMiddleware,
    validateJoiRequest({ paramsSchema: reviewIdValidation }),
    validateJoiRequest({ bodySchema: updateReviewValidation }),
    updateReviewById,
  )
  .delete(
    authMiddleware,
    validateJoiRequest({ paramsSchema: reviewIdValidation }),
    deleteReviewById,
  );

reviewRouter.route('*').all(methodNotAllowed);

export default reviewRouter;