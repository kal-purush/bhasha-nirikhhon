import { Router } from 'express';
import postsController from '../controllers/postsController';
import { upload } from '../dbs/mongoDb';
import { active } from '../utils/activeMiddleware';
import { auth } from '../utils/authMiddleware';

const router = Router(); 

router.put('/:id', auth, active, upload.single('photo'), postsController.updatePost);

router.delete('/:id', auth, active, postsController.deletePost);

router.post('/', auth, active, upload.single('photo'), postsController.createPost);

router.get('/', postsController.readPosts);

router.get('/:id', postsController.readPost);

export default router;