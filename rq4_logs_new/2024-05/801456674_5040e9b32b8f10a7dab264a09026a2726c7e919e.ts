import { Router } from "express";
import { AcademicFacultyControllers } from "./faculty.controller";


const router = Router()


router.post('/create-faculty', AcademicFacultyControllers.createAcdemicFacultyController)