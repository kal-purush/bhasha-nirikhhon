import {Injectable} from '@angular/core';
import {Observable, of} from 'rxjs';
import {Course} from './course';

@Injectable({
  providedIn: 'root'
})
export class CourseService {

  private mockCourses: Course[] = [
    new Course('1', 'Video course 1', new Date('2020-10-11T03:24:00'), 120, 'These Angular docs help you learn and use the Angular framework and development platform, from your first application to optimizing complex single-page apps for enterprises. Tutorials and guides include downloadable examples to accelerate your projects.'),
    new Course('2', 'Video course 2', new Date('2020-10-12T03:24:00'), 80, 'These Angular docs help you learn and use the Angular framework and development platform, from your first application to optimizing complex single-page apps for enterprises. Tutorials and guides include downloadable examples to accelerate your projects.'),
    new Course('3', 'Video course 3', new Date('2020-10-13T03:24:00'), 30, 'These Angular docs help you learn and use the Angular framework and development platform, from your first application to optimizing complex single-page apps for enterprises. Tutorials and guides include downloadable examples to accelerate your projects.')
  ];

  getAll(): Observable<Course[]> {
    return of(this.mockCourses);
  }

  getById(id: string): Observable<Course> {
    console.log('get course By Id - ' + id);
    return of(this.mockCourses.find(course => course.id === id));
  }

  add(course: Course): Observable<Course> {
    course.id = Math.round(Math.random() * 1000).toString();
    this.mockCourses.push(course);
    console.log('add course - ' + course);
    return of(course);
  }

  update(course: Course): Observable<Course> {
    this.mockCourses.map(mockCourse => mockCourse.id === course.id ? course : mockCourse);
    console.log('update course - ' + course);
    return of(course);
  }

  delete(id: string): void {
    this.mockCourses = this.mockCourses.filter(course => course.id !== id);
    console.log('deleted course id - ' + id);
  }
}