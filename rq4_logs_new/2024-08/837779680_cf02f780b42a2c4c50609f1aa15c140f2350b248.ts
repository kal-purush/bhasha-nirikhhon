import { Router } from 'express';
import {
  toggleProductToUserWishlist,
  getUserWishList,
} from '../controllers/wishListsController';
import { methodNotAllowed } from '../controllers/suspicionController';

import validateJoiRequest from '../middlewares/validateJoiRequest';
import { wishlistIdValidation , wishListProductIdValidation} from '../validators/wishlistFieldsValidation';

import authMiddleware from '../middlewares/authMiddleware';

const wishListRouter = Router();

wishListRouter.route('/')
  .post(
    authMiddleware,
    validateJoiRequest({ bodySchema: wishListProductIdValidation }),
    toggleProductToUserWishlist,
  )
  .get(
    authMiddleware,
    validateJoiRequest({ paramsSchema: wishlistIdValidation }),
    getUserWishList,

  );

// wishlistRouter.route('/:id')
//   .get(
//     authMiddleware,
//     validateJoiRequest({ paramsSchema: wishlistIdValidation }),
//     getReviewById,
//   )
//   .delete(
//     authMiddleware,
//     validateJoiRequest({ paramsSchema: wishlistIdValidation }),
//     deleteReviewById,
//   );

wishListRouter.route('*').all(methodNotAllowed);

export default wishListRouter;