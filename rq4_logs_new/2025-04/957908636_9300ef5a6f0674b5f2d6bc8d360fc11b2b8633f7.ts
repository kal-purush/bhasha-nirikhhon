import { a2pController } from '@controllers/v2/a2p-controller';
import { otpController } from '@controllers/v2/otp-controller';

import express from 'express';

const router = express();

/**
 * @swagger
 * /a2p:
 *   post:
 *     description: Send message from your application
 *     responses:
 *       200:
 *         description: Returns a message id
 */
router.post('/a2p', a2pController);

/**
 * @swagger
 * /otp:
 *   post:
 *     description: Send OTP to user
 *     responses:
 *       200:
 *         description: Returns a message id
 */
router.post('/otp', otpController.sendOTP);
/**
 * @swagger
 * /otp/verify:
 *   post:
 *     description: Verifies otp against given number
 *     responses:
 *       200:
 *         description: Returns validity of otp
 */
router.post('/otp/verify', otpController.verifyOTP);

export default router;