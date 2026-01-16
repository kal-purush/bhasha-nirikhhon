import { Request, Response } from 'express'
import { apiResponse } from '~/utils/apiResponse'
import { HttpStatus } from '~/utils/httpStatus'
import { AddServiceType } from '~/type/service.type'
import { ServiceService } from '~/services/service.service'

/**
 * @swagger
 * tags:
 *   name: Service
 *   description: API service
 */

export class ServiceController {
  /**
   * @swagger
   * /api/service/add:
   *   post:
   *     summary: Thêm mới dịch vụ
   *     description: Thêm mới dịch vụ
   *     tags:
   *       - Service
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             properties:
   *               name:
   *                 type: string
   *               price:
   *                 type: number
   *               description:
   *                 type: string
   *     responses:
   *       201:
   *         description: Thêm mới thành công
   *       400:
   *         description: Dữ liệu không hợp lệ
   *       500:
   *         description: Lỗi máy chủ
   */

  static async addService(req: Request, res: Response) {
    try {
      const service: AddServiceType = req.body
      const result: any = await ServiceService.addService(service)
      res.json(apiResponse(HttpStatus.CREATED, 'Tạo mới dịch vụ thành công', result))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/service:
   *   get:
   *     summary: Lấy danh sách dịch vụ
   *     description: API để lấy danh sách dịch vụ với phân trang, tìm kiếm, và sắp xếp
   *     tags:
   *       - Service
   *     parameters:
   *       - in: query
   *         name: page
   *         schema:
   *           type: integer
   *         description: Trang hiện tại
   *       - in: query
   *         name: limit
   *         schema:
   *           type: integer
   *         description: Số lượng dịch vụ trên mỗi trang
   *       - in: query
   *         name: search
   *         schema:
   *           type: string
   *         description: Từ khóa tìm kiếm
   *       - in: query
   *         name: sort
   *         schema:
   *           type: string
   *         description: Trường để sắp xếp
   *       - in: query
   *         name: order
   *         schema:
   *           type: string
   *           enum: [ASC, DESC]
   *         description: Thứ tự sắp xếp
   *     responses:
   *       200:
   *         description: Danh sách dịch vụ trả về thành công
   *       500:
   *         description: Lỗi máy chủ
   */
  static async getAllServices(req: Request, res: Response) {
    try {
      const { page = 1, limit = 10, search = '', sort = 'createdAt', order = 'DESC' } = req.query
      const result = await ServiceService.getAllServices(
        Number(page),
        Number(limit),
        String(search),
        String(sort),
        String(order)
      )
      res.json(apiResponse(HttpStatus.OK, 'Lấy danh sách dịch vụ thành công', result))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/service/{id}:
   *   get:
   *     summary: Lấy chi tiết dịch vụ
   *     description: API để lấy thông tin chi tiết của một dịch vụ
   *     tags:
   *       - Service
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *         description: ID của dịch vụ cần lấy thông tin
   *     responses:
   *       200:
   *         description: Trả về thông tin dịch vụ thành công
   *       404:
   *         description: Dịch vụ không tồn tại
   *       500:
   *         description: Lỗi máy chủ
   */
  static async getServiceById(req: Request, res: Response) {
    try {
      const { id } = req.params
      const result = await ServiceService.getServiceById(id)
      if (!result) {
        res
          .status(HttpStatus.BAD_REQUEST)
          .json(apiResponse(HttpStatus.BAD_REQUEST, 'Dịch vụ không tồn tại', null, true))
      } else {
        res.json(apiResponse(HttpStatus.OK, 'Lấy thông tin dịch vụ thành công', result))
      }
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/service/update/{id}:
   *   put:
   *     summary: Cập nhật dịch vụ
   *     description: Cập nhật thông tin của một dịch vụ
   *     tags:
   *       - Service
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             properties:
   *               name:
   *                 type: string
   *               price:
   *                 type: number
   *               description:
   *                 type: string
   *     responses:
   *       200:
   *         description: Cập nhật thành công
   *       400:
   *         description: Dữ liệu không hợp lệ
   *       500:
   *         description: Lỗi máy chủ
   */
  static async updateService(req: Request, res: Response) {
    try {
      const { id } = req.params
      const serviceData: AddServiceType = req.body
      const result = await ServiceService.updateService(id, serviceData)
      res.json(apiResponse(HttpStatus.OK, 'Cập nhật dịch vụ thành công', result))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/service/delete/{id}:
   *   delete:
   *     summary: Xóa dịch vụ
   *     description: Xóa một dịch vụ theo ID
   *     tags:
   *       - Service
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *     responses:
   *       200:
   *         description: Xóa thành công
   *       400:
   *         description: Không tìm thấy dịch vụ
   *       500:
   *         description: Lỗi máy chủ
   */
  static async deleteService(req: Request, res: Response) {
    try {
      const { id } = req.params
      await ServiceService.deleteService(id)
      res.json(apiResponse(HttpStatus.OK, 'Xóa dịch vụ thành công', null))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }
}