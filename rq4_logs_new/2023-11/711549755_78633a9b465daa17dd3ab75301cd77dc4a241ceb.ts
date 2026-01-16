import { Grade, Lecturer } from './customTypesHw02';

class Lesson {
  private _date: Date;

  constructor(date: Date) {
    this._date = date;
  }

  get date(): Date {
    return this._date;
  }

  set date(newDate: Date) {
    this._date = newDate;
  }
}

type Visit = {
  lesson: Lesson;
  present: boolean;
};

class Student {
  // implement 'set grade' and 'set visit' methods

  private _firstName: string;

  private _lastName: string;

  private _birthYear: number;

  private _grades: Grade = {}; // workName: mark

  private _visits: Visit[]; // lesson: present

  constructor(firstName: string, lastName: string, birthYear: number) {
    this._firstName = firstName;
    this._lastName = lastName;
    this._birthYear = birthYear;
    this._grades = {};
    this._visits = [];
  }

  get fullName(): string {
    return `${this._lastName} ${this._firstName}`;
  }

  set fullName(value) {
    [this._lastName, this._firstName] = value.split(' ');
  }

  get age(): number {
    return new Date().getFullYear() - this._birthYear;
  }

  getPerformanceRating(): number {
    const gradeValues = Object.values(this._grades);

    if (!gradeValues.length) {
      return 0;
    }

    const averageGrade = gradeValues.reduce((sum, grade) => sum + grade, 0) / gradeValues.length;
    const attendancePercentage = (this._visits.filter((visit) => visit.present).length / this._visits.length) * 100;

    return (averageGrade + attendancePercentage) / 2;
  }

  set grade(newGrades: Grade) {
    for (const [workName, mark] of Object.entries(newGrades)) {
      if (this._grades[workName] !== undefined) {
        throw new Error(`The grade for ${workName} is already present.`);
      } else {
        this._grades[workName] = mark;
      }
    }
  }

  set visit(visit: Visit) {
    const isVisitRecorded = this._visits.some((v) => v.lesson.date.getTime() === visit.lesson.date.getTime());
    if (isVisitRecorded) {
      throw new Error(`The visit for the lesson on ${visit.lesson.date} is already recorded.`);
    }
    this._visits.push(visit);
  }
}

class Level {
  // implement getters for fields and 'add/remove group' methods

  private _groups: string[] = [];

  private _name: string;

  private _description: string;

  constructor(name: string, description: string) {
    this._name = name;
    this._description = description;
  }

  get name(): string {
    return this._name;
  }

  get description(): string {
    return this._description;
  }

  get groups(): string[] {
    return this._groups;
  }

  addGroup(group: string): void {
    if (!this._groups.includes(group)) {
      this._groups.push(group);
    } else {
      throw new Error(`the group ${group} is already present`);
    }
  }

  removeGroup(group: string): string {
    const valueIndex = this._groups.indexOf(group);
    if (valueIndex === -1) {
      throw new Error(`there no group ${group} in groups array`);
    } else {
      this._groups.splice(valueIndex, 1);
      return 'group ${group} successfully deleted';
    }
  }
}

class Area {
  // implement getters for fields and 'add/remove level' methods
  private _levels: Level[] = [];

  private _name: string;

  constructor(name: string) {
    this._name = name;
  }

  get levels(): Level[] {
    return this._levels;
  }

  addLevel(level: Level): void {
    if (this._levels.includes(level)) {
      throw new Error(`the level ${level} is already exist`);
    }
    this._levels.push(level);
  }

  removeLevel(level: Level): string {
    const levelIndex = this._levels.indexOf(level);
    if (levelIndex === -1) {
      throw new Error(`the level ${level} is no present`);
    }
    this._levels.splice(levelIndex, 1);
    return `the level ${level.name} was removed`;
  }

  get name(): string {
    return this._name;
  }
}

class School {
  // implement 'add area', 'remove area', 'add lecturer', and 'remove lecturer' methods

  private _areas: Area[] = [];

  private _lecturers: Lecturer[] = []; // Name, surname, position, company, experience, courses, contacts

  get areas(): Area[] {
    return this._areas;
  }

  set areas(value: Area[]) {
    this._areas = value;
  }

  addArea(area: Area): void {
    if (!this._areas.includes(area)) {
      this._areas.push(area);
    } else {
      throw new Error(`the area ${area} is already present`);
    }
  }

  removeArea(area: Area): string {
    const areaIndex = this._areas.indexOf(area);
    if (areaIndex === -1) {
      throw new Error(`the area ${area.name} is no present`);
    }
    this._areas.splice(areaIndex, 1);
    return `the area ${area.name} was removed`;
  }

  get lecturers(): Lecturer[] {
    return this._lecturers;
  }

  set lecturers(value: Lecturer[]) {
    this._lecturers = value;
  }

  addLecturer(lecturer: Lecturer): void {
    if (!this._lecturers.includes(lecturer)) {
      this._lecturers.push(lecturer);
    } else {
      throw new Error(`the lecturer ${lecturer} is already present`);
    }
  }

  removeLecturer(lecturer: Lecturer): string {
    const lecturerIndex = this._lecturers.indexOf(lecturer);
    if (lecturerIndex === -1) {
      throw new Error(`the lecturer ${lecturer} is not present`);
    } else {
      this._lecturers.splice(lecturerIndex, 1);
      return `the lecturer ${lecturer} was removed`;
    }
  }
}

class Group {
  // implement getters for fields and 'add/remove student' and 'set status' methods

  private _area: Area;

  private _status: string[];

  private _students: Student[] = []; // Modify the array so that it has a valid toSorted method*

  private _directionName: string;

  private _levelName: string;

  constructor(directionName: string, levelName: string) {
    this._directionName = directionName;
    this._levelName = levelName;
  }

  get directionName(): string {
    return this._directionName;
  }

  get levelName(): string {
    return this._levelName;
  }

  get status(): string[] {
    return this._status;
  }

  set status(newStatus: string[]) {
    this._status = newStatus;
  }

  get students(): Student[] {
    return this._students;
  }

  addStudent(student: Student): void {
    this._students.push(student);
  }

  removeStudent(student: Student): void {
    const index = this._students.indexOf(student);
    if (index !== -1) {
      this._students.splice(index, 1);
    }
  }

  showPerformance(): Student[] {
    // const sortedStudents = this._students.toSorted((a, b) => b.getPerformanceRating() - a.getPerformanceRating());
    // ((
    const sortedStudents = [...this._students].sort((a, b) => b.getPerformanceRating() - a.getPerformanceRating());

    return sortedStudents;
  }
}