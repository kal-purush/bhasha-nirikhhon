import {Router } from "express";

const router = Router();

router.use('/', appIndex);

function appIndex(req, res, next) {
  req.initialOptions = {
    lang: 'en',
	};
	req.initialData = {
		app:{
      title: 'hello'
    }
	};
	next();
}

export default router;