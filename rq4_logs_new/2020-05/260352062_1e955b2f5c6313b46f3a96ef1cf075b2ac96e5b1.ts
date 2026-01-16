import { Request, Response, NextFunction } from 'express';
import IHomeController from '@controllers/Frontend/HomeController/IHomeController';

import UserModel from '@models/User';
import Controller from '@controllers/Controller';

class HomeController extends Controller implements IHomeController {
    constructor() {
        super();
        this.Index = this.Index.bind(this);
    }

    public async Index(req: Request, res: Response, next: NextFunction): Promise<void> {
        const users = await UserModel.all();
        console.log(users);
        return res.render('index', {
            pageTitle: "Home | My Company",
            pageMeta: {
                title: "Kadek Pradnyana",
                description: "Hello bor",
                keyword: "hell, yeah"
            }
        });
    }

    public Course(req: Request, res: Response, next: NextFunction): void {
        return res.render('frontend/course');
    }
    
    public Register(req: Request, res: Response, next: NextFunction): void {
        return res.render('frontend/register');
    }
}

export default HomeController;