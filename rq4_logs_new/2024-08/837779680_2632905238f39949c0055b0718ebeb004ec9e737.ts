import { Router } from 'express';
import { createCategory } from '../controllers/categoriesController';
import { methodNotAllowed } from '../controllers/suspicionController';
import categoryValidation from '../validators/categoryFieldsValidation';
import validateJoiRequest from '../middlewares/validateJoiRequest';

import authMiddleware  from '../middlewares/authMiddleware';
import adminMiddleware from '../middlewares/adminMiddleware';

const categoryRouter = Router();

categoryRouter.use(authMiddleware);
categoryRouter.use(adminMiddleware);
categoryRouter.route('/create').post(validateJoiRequest(categoryValidation), createCategory);

categoryRouter.route('*').all(methodNotAllowed);

export default categoryRouter;