import Router from '@koa/router';
import {
  getToy,
  getToyList,
  createToy,
  editToy,
  uploadToyImage,
  deleteToy,
} from './toy.controller';

const router = new Router({
  prefix: `/api/toys`,
});

router
  .get('/:toyId', getToy)
  .get('/', getToyList)
  .post('/', createToy)
  .patch('/:toyId', editToy)
  .patch('/:toyId/upload_image', uploadToyImage)
  .delete('/:toyId', deleteToy);

// adding custom settings for middleware of upload image routes
// app.use("/projects/:id/upload_image", express.json({ limit: "50mb" }));
// app.use("/projects/:id/upload_image", express.urlencoded({ extended: "true" }));

export default router;