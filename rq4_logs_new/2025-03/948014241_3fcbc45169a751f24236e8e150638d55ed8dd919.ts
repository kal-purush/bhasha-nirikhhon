import { Request, Response } from 'express'
import { apiResponse } from '~/utils/apiResponse'
import { HttpStatus } from '~/utils/httpStatus'
import { SpecialtyService } from '~/services/specialty.service'

/**
 * @swagger
 * tags:
 *   name: Specialty
 *   description: API chuyên khoa
 */

export class SpecialtyController {
  /**
   * @swagger
   * /api/specialty/add:
   *   post:
   *     summary: Thêm mới chuyên khoa
   *     description: API để thêm một chuyên khoa mới
   *     tags:
   *       - Specialty
   *     requestBody:
   *       required: true
   *       content:
   *         multipart/form-data:
   *           schema:
   *             type: object
   *             properties:
   *               name:
   *                 type: string
   *               file:
   *                 type: string
   *                 format: binary
   *     responses:
   *       201:
   *         description: Thêm mới chuyên khoa thành công
   *       400:
   *         description: Dữ liệu không hợp lệ
   *       500:
   *         description: Lỗi máy chủ
   */
  static async createSpecialty(req: Request, res: Response) {
    try {
      if (!req.file) {
        res.status(400).json(apiResponse(HttpStatus.BAD_REQUEST, 'Ảnh chuyên khoa là bắt buộc', null, true))
        return
      }

      const { name } = req.body
      if (!name) {
        res.status(400).json(apiResponse(HttpStatus.BAD_REQUEST, 'Tên chuyên khoa là bắt buộc', null, true))
        return
      }

      const specialty = await SpecialtyService.createSpecialty(name, req.file)
      res.json(apiResponse(HttpStatus.CREATED, 'Tạo mới chuyên khoa thành công', specialty))
    } catch (error: any) {
      console.error('Error in createSpecialty:', error)
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/specialty:
   *   get:
   *     summary: Lấy danh sách chuyên khoa
   *     description: API để lấy danh sách chuyên khoa với phân trang, tìm kiếm, và sắp xếp
   *     tags:
   *       - Specialty
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
   *         description: Danh sách chuyên khoa trả về thành công
   *       500:
   *         description: Lỗi máy chủ
   */
  static async getAllSpecialties(req: Request, res: Response) {
    try {
      const { page = 1, limit = 10, search = '', sort = 'createdAt', order = 'DESC' } = req.query
      const result = await SpecialtyService.getAllSpecialties(
        Number(page),
        Number(limit),
        String(search),
        String(sort),
        String(order)
      )
      res.json(apiResponse(HttpStatus.OK, 'Lấy danh sách chuyên khoa thành công', result))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/specialty/{id}:
   *   get:
   *     summary: Lấy chi tiết chuyên khoa
   *     description: API lấy thông tin chi tiết của một chuyên khoa
   *     tags:
   *       - Specialty
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *     responses:
   *       200:
   *         description: Trả về thông tin chuyên khoa thành công
   *       404:
   *         description: Chuyên khoa không tồn tại
   *       500:
   *         description: Lỗi máy chủ
   */
  static async getSpecialtyById(req: Request, res: Response) {
    try {
      const { id } = req.params
      const result = await SpecialtyService.getSpecialtyById(id)
      if (!result) {
        res
          .status(HttpStatus.BAD_REQUEST)
          .json(apiResponse(HttpStatus.BAD_REQUEST, 'Chuyên khoa không tồn tại', null, true))
      } else {
        res.json(apiResponse(HttpStatus.OK, 'Lấy thông tin chuyên khoa thành công', result))
      }
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/specialty/update/{id}:
   *   put:
   *     summary: Cập nhật chuyên khoa
   *     description: API để cập nhật thông tin chuyên khoa
   *     tags:
   *       - Specialty
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *     requestBody:
   *       required: true
   *       content:
   *         multipart/form-data:
   *           schema:
   *             type: object
   *             properties:
   *               name:
   *                 type: string
   *               file:
   *                 type: string
   *                 format: binary
   *     responses:
   *       200:
   *         description: Cập nhật thành công
   *       400:
   *         description: Dữ liệu không hợp lệ
   *       500:
   *         description: Lỗi máy chủ
   */
  static async updateSpecialty(req: Request, res: Response) {
    try {
      const { id } = req.params
      const { name } = req.body
      const file = req.file

      if (!name) {
        res.status(400).json(apiResponse(HttpStatus.BAD_REQUEST, 'Tên chuyên khoa là bắt buộc', null, true))
        return
      }
      if (!file) {
        res.status(400).json(apiResponse(HttpStatus.BAD_REQUEST, 'Ảnh chuyên khoa là bắt buộc', null, true))
        return
      }

      const result = await SpecialtyService.updateSpecialty(id, name, file)
      res.json(apiResponse(HttpStatus.OK, 'Cập nhật chuyên khoa thành công', result))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }

  /**
   * @swagger
   * /api/specialty/delete/{id}:
   *   delete:
   *     summary: Xóa chuyên khoa
   *     description: API để xóa chuyên khoa theo ID
   *     tags:
   *       - Specialty
   *     parameters:
   *       - in: path
   *         name: id
   *         required: true
   *         schema:
   *           type: string
   *     responses:
   *       200:
   *         description: Xóa chuyên khoa thành công
   *       404:
   *         description: Chuyên khoa không tồn tại
   *       500:
   *         description: Lỗi máy chủ
   */
  static async deleteSpecialty(req: Request, res: Response) {
    try {
      const { id } = req.params
      await SpecialtyService.deleteSpecialty(id)
      res.json(apiResponse(HttpStatus.OK, 'Xóa chuyên khoa thành công', null))
    } catch (error: any) {
      res
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .json(apiResponse(HttpStatus.INTERNAL_SERVER_ERROR, error.message, null, true))
    }
  }
}