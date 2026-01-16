import { Request, RequestHandler, Response } from 'express'
import { UserService } from '../services/user.service'
import { apiResponse } from '~/utils/apiResponse'
import { HttpStatus } from '~/utils/httpStatus'
import { RegisterType } from '~/type/auth.type'
import { AuthService } from '~/services/auth.service'

/**
 * @swagger
 * tags:
 *   name: Auth
 *   description: API xác thực người dùng
 */

export class AuthController {
  /**
   * @swagger
   * /api/auth/register:
   *   post:
   *     summary: Đăng ký tài khoản
   *     description: Đăng ký tài khoản người dùng mới
   *     tags:
   *       - Auth
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             properties:
   *               userName:
   *                 type: string
   *               email:
   *                 type: string
   *               phone:
   *                 type: string
   *               password:
   *                 type: string
   *               confirmPassword:
   *                 type: string
   *     responses:
   *       201:
   *         description: Đăng ký thành công
   *       400:
   *         description: Dữ liệu không hợp lệ
   *       500:
   *         description: Lỗi máy chủ
   */

  static async register(req: Request, res: Response) {
    try {
      const user: RegisterType = req.body
      const result: any = await AuthService.registerService(user)
      res.json(apiResponse(HttpStatus.CREATED, 'Đăng kí thành công, vui lòng kích hoạt tài khoản qua email', result))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }
}