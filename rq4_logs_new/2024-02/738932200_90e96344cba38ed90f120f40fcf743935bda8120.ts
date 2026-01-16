import request from 'supertest';
import app from './Server'; // Import your Express app
import * as studentService from './services/Student'; // Import the studentService module


jest.mock('./services/Student'); // Mock studentService

describe('getStudents', () => {
  it('should return a list of students', async () => {
    const students = [{ id: 1, name: 'Alice' }];
     (studentService.getStudentsService as jest.Mock).mockResolvedValue(students);
       request(app).get('/').expect(200);

  
   });



});
describe('createStudent', () => {
  it('should create a new student when request is valid', async () => {
    const newStudentData = { name: 'Bob', age: 25 };
    const createdStudent = { id: 1, ...newStudentData };
    //studentService.createStudentService = jest.fn().mockResolvedValue(createdStudent);
    request(app).post('/').expect(200);

    });
 
});
describe('updateStudent', () => {
  it('should update an existing student with valid data', async () => {
    const studentId = 1;
    const updatedData = { name: 'Charlie' };
    const updatedStudent = { id: studentId, ...updatedData };
   // studentService.updateStudentService = jest.fn().mockResolvedValue(updatedStudent);
    request(app).put('/1').expect(200);
    
  });
});

describe('deleteStudent', () => {
  it('should delete a student', async () => {
    //const studentId = 1;
    const successMessage = 'Student deleted successfully';
    jest.spyOn(studentService, 'deleteStudentService').mockResolvedValue(successMessage);
    request(app).delete('/1').expect(200);

    
  });

});