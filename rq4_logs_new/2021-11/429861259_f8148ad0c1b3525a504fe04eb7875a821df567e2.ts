import { Router } from 'express';
import * as AccountControllers from './account.controllers';
import { ensureUserIsAuthenticated } from '../../middleware/auth.middleware';
import { validateSchema } from '../../middleware/validation.middleware';
import { AccountInputSchema } from './account.schema';

const router = Router({ mergeParams: true });

router.post(
	'/link',
	[ensureUserIsAuthenticated, validateSchema(AccountInputSchema)],
	AccountControllers.loginAccountHandler
);

export default router;