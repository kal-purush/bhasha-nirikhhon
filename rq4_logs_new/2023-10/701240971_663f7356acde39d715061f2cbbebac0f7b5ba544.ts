import express, { Express, Request, Response, NextFunction } from "express";
import {checkAuthenticated, checkNotAuthenticated} from '../middlewares/auth';
const mainRouter = express.Router();

//회원 가입 form 뷰
mainRouter.get('/signup', checkNotAuthenticated, (req: Request, res: Response) => {
    res.render('signup');
});

//로그인 화면 form 뷰
mainRouter.get('/signin', checkNotAuthenticated, (req: Request, res: Response) => {
    res.render('signin');
});

//인증된 회원만 볼 수 있는 뷰
mainRouter.get('/', checkAuthenticated, async (req: Request, res: Response) => {
    res.render('index');
});

export default mainRouter;