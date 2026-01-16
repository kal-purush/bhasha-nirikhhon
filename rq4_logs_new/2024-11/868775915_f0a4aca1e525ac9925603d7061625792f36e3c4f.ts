
import { logger } from "../../logger/Logger";
import { Request, response, Response } from "express";
import { _200, _201, _400, _404 } from "../../utils/ApiResponse";
import { createOtherTax, getOtherTaxById, getOtherTaxList, getTotalOtherTaxCount, softDeleteOtherTax, updateOtherTax } from "../../services/admin/other-tax.service";
import { createBDOUser, getBDOUserById, getBDOUserList, getTotalBDOUserCount, softDeleteBDOUser, updateBDOUser } from "../../services/admin/bdo-user.service";

export class BDOUser {
    static async addBDOUser(req: Request, res: Response) {
        try {
            await createBDOUser(req.body);
            return _201(res, "BDO User created successfully");
        } catch (error) {
            logger.error(error);
            return _400(res, error.message);
        }
    }

    static async updateBDOUserInfo(req: Request, res: Response) {
        try {
            const { id } = req.body;
            if (!id) {
                return _400(res, "BDO User ID is required");
            }
            let isExists = await getBDOUserById(id);
            if (!isExists) {
                return _404(res, "BDO User details not found.");
            }
            
            const result = await updateBDOUser(req.body);
            return _200(res, "BDO User updated successfully", result);
        } catch (error) {
            logger.error(error);
            return _400(res, error.message);
        }
    }

    static async getAllBDOUser(req: Request, res: Response) {
        try {
            let response = [];
            const { page_number, search_text } = req.body;

            const result = await getBDOUserList(Number(page_number - 1), search_text as string);
            if(search_text){
                response['totalRecords'] = await getTotalBDOUserCount(search_text);
            }else{
                response['totalRecords'] = await getTotalBDOUserCount();
            }
            
            response['data'] = result;
            return _200(res, "BDO User list retrieved successfully", response);
        } catch (error) {
            logger.error(error);
            return _400(res, error.message);
        }
    }

    static async getBDOUser(req: Request, res: Response) {
        try {
            const { id } = req.params;
            if (!id) {
                return _400(res, "BDO User ID is required");
            }
            const result = await getBDOUserById(Number(id));
            if (!result) {
                return _404(res, "BDO User not found");
            }
            response['data'] = result;
            return _200(res,"BDO User data fetch successfully",response);
        } catch (error) {
            logger.error(error);
            return _400(res, error.message);
        }
    }

    static deleteBDOUser = async (req, res, next) => {
        try {
            const { id } = req.params;
            if (!id) {
                return _400(res, "BDO User ID is required");
            }
            const result = await getBDOUserById(Number(id));
            if (!result) {
                return _404(res, "BDO User not found");
            }
            let response: any = await softDeleteBDOUser(id);
            if (response) {
                return _200(res, "BDO User deleted successfully");
            } else {
                return _400(res, "BDO User not deleted,Something went wrong.");
            }
        } catch (error) {
            logger.error(error);
            return _400(res, error.message);
        }
    }

}




