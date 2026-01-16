import { RequestHandler } from 'express';
import { academicSemesterServices } from './academicSemester.services';

const createAcademicSemesterController: RequestHandler = async (
  req,
  res,
  next
) => {
  try {
    const { ...academicSemester } = req.body;

    const result = await academicSemesterServices.createAcademicSemester(
      academicSemester
    );

    res.status(200).json({
      success: true,
      message: 'Academic Semester Created Successfully',
      data: result,
    });
  } catch (error) {
    next(error);
  }
};

export const academicSemesterController = {
  createAcademicSemesterController,
};