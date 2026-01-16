import { Course } from '../models/course';

import * as WebRequest from 'web-request';

export class CourseraFetcher {
  async getAllCourses(): Promise<Course[]> {
    let url = 'https://www.coursera.org/maestro/api/topic/list?full=1';
    console.log("Starting Coursera Request!");
    let courseData = await WebRequest.json<any[]>(url);
    try {
      let courses = courseData.map(course => {
        return new Course(course.name,
                          course.short_description,
                          this.validateLink(course.courses) ? course.courses[0].home_link : "");
      });
      return courses;
    } catch (e) {
      console.log(e);
      return [];
    }
  }

  validateLink(courses: any[]): boolean {
    return courses != undefined && courses.length > 0;
  }
}