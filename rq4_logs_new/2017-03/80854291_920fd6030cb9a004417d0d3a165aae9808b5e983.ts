import 'es6-shim';
import 'reflect-metadata';
import { expect } from 'chai';
import 'mocha';
import { CourseMockGateway } from "../data/gateways/course.gateway.mock";
import { CourseUseCases } from "./course.use-case";
import { Course } from "./course";

describe('Get courses use case', () => {
  it('should return messages', () => {
    let courseMockGateway = new CourseMockGateway();
    let courseUseCases = new CourseUseCases(courseMockGateway);
    return courseUseCases.getCourses().then((coursesActual: Course[]) => {
      return courseMockGateway.getAllCourses().then((coursesExpected: Course[]) => {
        expect(coursesActual.length).to.be.equal(coursesExpected.length);
      });
    });
  });
});