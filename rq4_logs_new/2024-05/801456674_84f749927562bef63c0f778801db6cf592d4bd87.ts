import { catchasync } from "../../utils/catchAsync";
import { AcademicFacultyServices } from "./faculty.service";


const createAcdemicFacultyController = catchasync(
    async (req, res) => {

        const { faculty } = req.body;
        const result = await AcademicFacultyServices.createAcdemicFacultyDB(faculty)

        res.status(200).json({
            success: true,
            message: "new faculty created",
            data: result
        })

    }
)



export const AcademicFacultyControllers = {
    createAcdemicFacultyController
}