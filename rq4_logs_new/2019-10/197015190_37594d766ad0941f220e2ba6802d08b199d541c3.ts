import { Injectable } from '@angular/core';
import { Actions, Effect, ofType } from '@ngrx/effects';
import { CourseService } from '../../@services/course.service';
import { ECourseActions, GetCourses, GetCoursesSuccess } from '../actions/course.actions';
import { of } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { Course } from '../../@models/course';

@Injectable()
export class CourseEffects {
  constructor(private actions$: Actions, private courseService: CourseService) {}

  @Effect() GetCourses$ = this.actions$.pipe(
    ofType(ECourseActions.GetCourses),
    switchMap(() => this.courseService.getCourse()),
    switchMap((courses: Course[]) => of(new GetCoursesSuccess(courses))),
  );
}