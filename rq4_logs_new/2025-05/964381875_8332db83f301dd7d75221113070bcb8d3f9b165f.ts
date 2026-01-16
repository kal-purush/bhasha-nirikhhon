import { Router } from 'express';
import userRouter from '@routes/v1/user';
import authRouter from '@routes/v1/auth';
import adminRouter from '@routes/v1/admin';
import frontpageRouter from '@routes/v1/frontpage';
import activityRouter from '@routes/v1/activity';

const router = Router();

// v1 路由可以在這裡添加
router.use('/user', userRouter);
router.use('/auth', authRouter);
router.use('/admin', adminRouter);
router.use('/frontpage', frontpageRouter);
router.use('/activity', activityRouter);

export default router;