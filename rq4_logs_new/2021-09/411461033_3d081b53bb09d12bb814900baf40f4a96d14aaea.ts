import { Request, Response } from 'express'
import { Controller, Get, Post, Req, Res } from 'routing-controllers'

@Controller("users/")
export default class UserControllers {
    constructor() {}

    @Get('login')
    public login(@Req() req: Request, @Res() res: Response) {
        return res.status(200).json({
            message: 'hello wolrds'
        })
    }

    @Post()
    public register(@Req() req: Request, @Res() res: Response) {
        return res.status(200).json({
            'message': 'hello worlds'
        })
    }
}