import { Request, Response } from 'express';
import prisma from '../prisma';

export async function createCourse(req: Request, res: Response) {
    let { name } = req.body;

    if (!name) return res.status(401).json("Course Name Cannot be Empty!");

    try {
        const course = await prisma.course.create({
            data: {
                name: name,
                instructor: {
                    connect: { id: res.locals.signedInUser.id }
                }
            },
            include: {
                instructor: true,
            }
        })
        res.status(201).json({ courseName: course.name, courseId: course.id, instructorId: course.instrutorId })
    } catch (error) {
        console.log(error);
        res.status(500).json('Something went wrong!')
    }
}

export async function getAllCourses(req: Request, res: Response) {
    try {
        const courses = await prisma.course.findMany({
            where: {
                instrutorId: res.locals.signedInUser.id
            },
            select: {
                id: true,
                name: true,
                instrutorId: true,
            }
        });
        res.status(200).json({ courses: courses });
    } catch (error) {
        console.log(error);
        res.status(400).json("Something Went Wrong!");
    }
}

export async function deleteCourse(req: Request, res: Response) {
    const { id } = req.params;

    try {
        const userCourses = await prisma.course.findMany({
            where: {
                AND: [
                    { instrutorId: res.locals.signedInUser.id },
                    { id: id }
                ]
            }
        });
        if (!userCourses || userCourses.length === 0) return res.status(401).json("Unauthorized!");

        const deletedCourse = await prisma.course.delete({
            where: {
                id: id
            }
        });
        res.status(200).json("Course Deleted!");
    } catch (error) {
        console.log(error);
        res.status(400).json("Something Went Wrong!");
    }
}

export async function getCourse(req: Request, res: Response) {
    const { id } = req.params;

    if (!id) return res.status(401).json("Course Id is required!");

    try {
        const course = await prisma.course.findFirst({
            where: {
                id: id
            },
            select: {
                name: true,
                instructor: true,
                instrutorId: true
            }
        });

        if (!course) return res.status(404).json("Course not Found!");

        res.status(200).json({
            courseName: course.name,
            instructorName: course.instructor.name
        });
    } catch (error) {
        console.log(error);
        res.status(500).json("something Went Wrong!");
    }

}