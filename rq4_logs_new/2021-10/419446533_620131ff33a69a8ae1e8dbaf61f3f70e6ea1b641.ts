import { Router } from 'express';

import { AuthenticateUserWithGithubController } from './controllers/AuthenticateUserWithGithubController';
import { CreateMessageController } from './controllers/CreateMessageController';
import { GetLastThreeMessagesController } from './controllers/GetLastThreeMessagesController';
import { GetUserProfileController } from './controllers/GetUserProfileController';

import { ensureAuthenticated } from './middlewares/ensureAuthenticated';

const routes = Router();

routes.post('/authenticate', new AuthenticateUserWithGithubController().handle);

routes.get('/profile', ensureAuthenticated, new GetUserProfileController().handle);

routes.post('/messages', ensureAuthenticated, new CreateMessageController().handle);

routes.get('/messages/last', new GetLastThreeMessagesController().handle);

export { routes };