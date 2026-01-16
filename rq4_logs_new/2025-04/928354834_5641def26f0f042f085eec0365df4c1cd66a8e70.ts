/**
 * @swagger
 * tags:
 *   name: EmailVerification
 *   description: Endpoints for email verification
 */

/**
 * @swagger
 * /send-code:
 *   post:
 *     summary: Send a verification code to the user's email
 *     tags: [EmailVerification]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - email
 *             properties:
 *               email:
 *                 type: string
 *                 description: The email address to send the verification code to
 *     responses:
 *       200:
 *         description: Verification code sent successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                   example: Verification code sent
 *       400:
 *         description: Email is required
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 error:
 *                   type: string
 *                   example: Email is required
 *       500:
 *         description: Failed to send verification email
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 error:
 *                   type: string
 *                   example: Failed to send verification email
 */

/**
 * @swagger
 * /verify-code:
 *   post:
 *     summary: Verify the code sent to the user's email
 *     tags: [EmailVerification]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - email
 *               - code
 *             properties:
 *               email:
 *                 type: string
 *                 description: The email address to verify
 *               code:
 *                 type: string
 *                 description: The verification code sent to the email
 *     responses:
 *       200:
 *         description: Email verified successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                   example: Email verified successfully
 *       400:
 *         description: Email and code are required, or the code is invalid/expired
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 error:
 *                   type: string
 *                   example: Invalid or expired verification code
 *       404:
 *         description: No verification record found for the email
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 error:
 *                   type: string
 *                   example: No verification record found
 */
// routes/emailVerification.ts
import prisma from '../app';
import logger from '../utilities/logger';
import { sendMail } from '../utilities/mailsender';
import { Router } from "express";


const router = Router();
router.post('/send-code', async (req: any, res: any) => {
  const { email } = req.body as { email: string };

  if (!email) return res.status(400).json({ error: 'Email is required' });

  const code = Math.floor(100000 + Math.random() * 900000).toString();

  try {
    await prisma.emailVerification.upsert({
      where: { email },
      update: { code, createdAt: new Date() },
      create: { email, code },
    });

    await sendMail(
      process.env.EMAIL_USERNAME!,
      email,
      'Your Verification Code',
      `Your verification code is: ${code}`,
      `<p>Your verification code is: <strong>${code}</strong></p>`
    );
    logger.info('Verification code sent');
    res.status(200).json({ message: 'Verification code sent' });
  } catch (err) {
    logger.error(err);
    res.status(500).json({ error: 'Failed to send verification email' });
  }
});

router.post('/verify-code', async (req: any, res: any) => {
  const { email, code } = req.body as { email: string; code: string };

  if (!email || !code)
    return res.status(400).json({ error: 'Email and code are required' });

  const record = await prisma.emailVerification.findUnique({ where: { email } });

  if (!record) return res.status(404).json({ error: 'No verification record found' });

  const isCodeValid = record.code === code;
  const isRecent =
    new Date().getTime() - new Date(record.createdAt).getTime() < 10 * 60 * 1000;

  if (isCodeValid && isRecent) {
    await prisma.emailVerification.delete({ where: { email } });
    return res.status(200).json({ message: 'Email verified successfully' });
  }

  return res.status(400).json({ error: 'Invalid or expired verification code' });
});

export default router;