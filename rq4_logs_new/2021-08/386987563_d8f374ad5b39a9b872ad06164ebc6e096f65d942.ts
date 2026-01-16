import axios from 'axios';
import { AuthorDTO } from '../models/Author';
import { CourseDTO } from '../models/Course';

export class ApiService {
  token = localStorage.getItem('token') ?? '';

  async addCourse(course: CourseDTO) {
    return await axios.post('http://localhost:3000/courses/add', course, {
      headers: {
        Authorization: JSON.parse(this.token),
      },
    });
  }

  async addAuthor(author: AuthorDTO) {
    return await axios.post('http://localhost:3000/authors/add', author, {
      headers: {
        Authorization: JSON.parse(this.token),
      },
    });
  }
}