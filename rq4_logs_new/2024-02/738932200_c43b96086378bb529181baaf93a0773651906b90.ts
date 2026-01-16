// import request from 'supertest';
// import express, { Application } from 'express';
// import { getStudents, createStudent, updateStudent, deleteStudent } from '../controllers/Student';
// import * as studentService from '../services/Student';


// // Mocking the express-validator validationResult function
// jest.mock('../middlewares/expressValidator/studentValidation', () => ({
//   validationResult: jest.fn(() => ({
//     isEmpty: jest.fn().mockReturnValue(false),
//     array: jest.fn().mockReturnValue([{ msg: 'Validation error' }]),
//   })),
// }));

// // Mocking the studentService functions
// jest.mock('../services/Student', () => ({
//   getStudentsService: jest.fn(),
//   createStudentService: jest.fn(),
//   updateStudentService: jest.fn(),
//   deleteStudentService: jest.fn(),
// }));

// const app: Application = express();
// app.use(express.json());

// app.get('/students', getStudents);
// app.post('/students', createStudent);
// app.put('/students/:id', updateStudent);
// app.delete('/students/:id', deleteStudent);



// describe('Student Controller Tests', () => {
//   // Test for getStudents
//   it('should get students successfully', async () => {
//     // Mocking the service function
//     //studentService.getStudentsService.mockResolvedValueOnce([{ id: 1, name: 'John Doe' }]);
//     (studentService.getStudentsService as jest.Mock).mockResolvedValueOnce([{ id: 1, name: 'John Doe' }]);
//     const response = await request(app).get('/students');
//     expect(response.status).toBe(200);
//     expect(response.body).toEqual([{ id: 1, name: 'John Doe' }]);
//   });

//   // Test for createStudent
//   it('should create a student successfully', async () => {
//     const reqBody = { name: 'Alice' };
//     // Mocking the service function
//    // studentService.createStudentService.mockResolvedValueOnce({ id: 2, name: 'Alice' });
//    (studentService.createStudentService as jest.Mock).mockResolvedValueOnce({ id: 2, name: 'Alice' });
//    const response = await request(app).post('/students').send(reqBody);
//     expect(response.status).toBe(200);
//     expect(response.body).toEqual({ id: 2, name: 'Alice' });
//   });

//   // Test for updateStudent
//   it('should update a student successfully', async () => {
//     const reqBody = { name: 'UpdatedName' };
//     // Mocking the service function
//     //studentService.updateStudentService.mockResolvedValueOnce({ id: 3, name: 'UpdatedName' });
//     (studentService.updateStudentService as jest.Mock).mockResolvedValueOnce({ id: 3, name: 'UpdatedName' });
//     const response = await request(app).put('/students/3').send(reqBody);
//     expect(response.status).toBe(200);
//     expect(response.body).toEqual({ id: 3, name: 'UpdatedName' });
//   });

//   // Test for deleteStudent
//   it('should delete a student successfully', async () => {
//     // Mocking the service function
//     //studentService.deleteStudentService.mockResolvedValueOnce('Student deleted successfully');
//     (studentService.deleteStudentService as jest.Mock).mockResolvedValueOnce('Student deleted successfully');
//     const response = await request(app).delete('/students/4');
//     expect(response.status).toBe(200);
//     expect(response.body).toEqual({ message: 'Student deleted successfully' });
//   });
// });

import { Request, Response } from 'express';
import * as studentService from '../services/Student';
import { createStudent, deleteStudent, getStudents } from '../controllers/Student'; // Import your controller function
//import { studentValidation } from 'src/middlewares/expressValidator/studentValidation';
import { validationResult } from 'express-validator';

jest.mock('../services/Student');// Mock the student service module
jest.mock('../routes/Student'); 
//jest.mock('../middlewares/expressValidator/studentValidation'); // Mock the express-validator module
//jest.mock('express-validator');


describe('getStudents', () => {
    let mockRequest: Partial<Request>;
    let mockResponse: Partial<Response>;

    beforeEach(() => {
        mockRequest = {};
        mockResponse = {
            status: jest.fn().mockReturnThis(),
            json: jest.fn(),
        };
    });
    beforeEach(() => {
        jest.clearAllMocks();
      });

    it('should return students with status 200 if service call is successful', async () => {
        const mockStudents = [{ id: 1, name: 'John Doe' }, { id: 2, name: 'Jane Doe' }];
        (studentService.getStudentsService as jest.Mock).mockResolvedValue(mockStudents);

        await getStudents(mockRequest as Request, mockResponse as Response);

        expect(mockResponse.status).toHaveBeenCalledWith(200);
        expect(mockResponse.json).toHaveBeenCalledWith(mockStudents);
    });

    it('should return 500 if an error occurs during service call', async () => {
        (studentService.getStudentsService as jest.Mock).mockRejectedValue(new Error('Service Error'));

        await getStudents(mockRequest as Request, mockResponse as Response);

        expect(mockResponse.status).toHaveBeenCalledWith(500);
        expect(mockResponse.json).toHaveBeenCalledWith({ error: 'Internal Server Error' });
    });


});

// describe('createStudent', () => {
//     let mockRequest: Partial<Request>;
//     let mockResponse: Partial<Response>;

//     beforeEach(() => {
//         mockRequest = {
//             body: {},
//         };
//         mockResponse = {
//             status: jest.fn().mockReturnThis(),
//             json: jest.fn(),
//         };
//     });

//     beforeEach(() => {
//         jest.clearAllMocks();
//     });
    

//     it('should return created student with status 200 if validation passes and service call is successful', async () => {
//         // Mock validation passing
//         (validationResult as jest.Mock).mockReturnValue({ isEmpty: jest.fn().mockReturnValue(true) });

//         const mockNewStudent = { id: 1, name: 'John Doe' };
//         (studentService.createStudentService as jest.Mock).mockResolvedValue(mockNewStudent);

//         await createStudent(mockRequest as Request, mockResponse as Response);

//         expect(mockResponse.status).toHaveBeenCalledWith(200);
//         expect(mockResponse.json).toHaveBeenCalledWith(mockNewStudent);
//     });

//     it('should return validation errors with status 400 if validation fails', async () => {
//         // Mock validation failing
//         (validationResult as jest.Mock).mockReturnValue({
//             isEmpty: () => false,
//             array: () => [{ msg: 'Name is required' }],
//         });

//         await createStudent(mockRequest as Request, mockResponse as Response);

//         expect(mockResponse.status).toHaveBeenCalledWith(400);
//         expect(mockResponse.json).toHaveBeenCalledWith({ errors: ['Name is required'] });
//     });

//     it('should return 500 if an error occurs during service call', async () => {
//         // Mock validation passing
//         (validationResult as jest.Mock).mockReturnValue({ isEmpty: () => true });

//         (studentService.createStudentService as jest.Mock).mockRejectedValue(new Error('Service Error'));

//         await createStudent(mockRequest as Request, mockResponse as Response);

//         expect(mockResponse.status).toHaveBeenCalledWith(500);
//         expect(mockResponse.json).toHaveBeenCalledWith({ error: 'Internal Server Error' });
//     });
// });

