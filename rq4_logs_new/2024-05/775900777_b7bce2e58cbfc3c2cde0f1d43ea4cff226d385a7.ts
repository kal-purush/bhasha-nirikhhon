import { AxiosError } from 'axios';

import backend from '../backend';
import { Success, Failure, Result } from '../../util/result';
import { Course } from '../../models/course';

// import backend from './backend';

export interface CourseSearchRequest {
  keyword: string;
}

export enum CourseSearchFailure {
  INIT,
  NO_RESULT,
  NO_AUTH,
  BAD_REQUEST,
}

export async function CourseSearchAPI(params: CourseSearchRequest): Promise<Result<Course[], CourseSearchFailure>> {
  const ret = await backend
    .get('/api/course/search', {
      params,
    })
    .then(
      (value) =>
        Promise.resolve(new Success(JSON.parse(value.data) as Course[])) as Promise<
          Result<Course[], CourseSearchFailure>
        >,
    )
    .catch((error: AxiosError) => {
      if (error.response) {
        switch (error.response.status) {
          case 404:
            return Promise.resolve(new Failure(CourseSearchFailure.NO_RESULT) as Result<Course[], CourseSearchFailure>);
          case 401:
            return Promise.resolve(new Failure(CourseSearchFailure.NO_AUTH) as Result<Course[], CourseSearchFailure>);
          case 400:
            return Promise.resolve(
              new Failure(CourseSearchFailure.BAD_REQUEST) as Result<Course[], CourseSearchFailure>,
            );
          default:
            return Promise.reject();
        }
      }
      return Promise.reject();
    });
  return ret;
}