import { Request, Response } from "express";
import { StudentService } from "./student.service";


const createStudents = async (req: Request, res: Response) => {
    try {
        const student = req.body;
        // StudentService.createStudentintoDB it is service for createstudent controller 
        const result = await StudentService.createStudentintoDB(student)

        res.status(200).json({
            data: result,
            message: "data success"
        })
    } catch (error) {
        console.log(error);

    }

}

export const StudentController = {
    createStudents
}