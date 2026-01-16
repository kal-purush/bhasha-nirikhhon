import express, { Router } from 'express';
import { getUserWithEmail, updatePassword, deleteAccount, logOutUser } from "../controllers/user";
import { withPermissions } from '../middleware/middleware';

const router = express.Router();

// GET request to get one user's profile
router.get('/get-user-profile/email', withPermissions(['View profile']), getUserWithEmail);

// PATCH request to update the user's password
router.patch('/update-password/email', withPermissions(["Update password"]), updatePassword);

// DELETE request to delete the user's account
router.delete('/delete-account/email', withPermissions(["Delete account"]), deleteAccount);

// POST request to log user out
router.post('/log-out/email', withPermissions(['View profile']), logOutUser);

export default router;