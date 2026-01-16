import { Router } from 'express';
import { User } from './index';

const router = Router();

router.get(`/${process.env.MAIN_DOMAIN}/user/:id/info`, async (req, res) => {
  const id = req.params?.id;
  if (!id) {
    return res.status(400).json({ message: 'Id is not passed' });
  }

  let userInfo = await User.findByExternalId(id);
  if (!userInfo) {
    userInfo = await User.findById(Number(id));
  }
  if (!userInfo) {
    return res.status(404).json({ message: 'User not found' });
  }

  return res.status(200).json(userInfo);
});

export default router;