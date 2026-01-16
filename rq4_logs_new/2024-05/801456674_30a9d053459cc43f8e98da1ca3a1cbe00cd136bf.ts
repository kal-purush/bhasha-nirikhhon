import { Request, Response } from "express";
import { catchasync } from "../../utils/catchAsync";
import { AcademicSemesterServices } from "./academic.service";




const createAcademicSemesterController = catchasync(
    async (req: Request, res: Response) => {
        const acamedicData = req.body;
        const result = await AcademicSemesterServices.createAcademicSemesterIntoDB(acamedicData)


        res.status(200).json({
            success: true,
            message: "AcademicSemester successfully created",
            data: result
        })
    }
)




export const AcademicSemesterControllers ={
    createAcademicSemesterController
}