import { AuthModel } from '../model/auth.model';
import { Component, OnInit } from '@angular/core';
import { StudentModel } from '../model/student.model';
import { RegisterModel } from '../model/register.model';
import { Student, Period, User } from './user.controller';

@Component({
  selector: 'app-entry-notes',
  templateUrl: '../view/entry-notes/entry-notes.view.html',
  styleUrls: ['../view/entry-notes/entry-notes.view.css']
})

export class EntryNotesComponent implements OnInit {

  period: Period;
  noteOne: number;
  noteTwo: number;
  noteFour: number;
  noteFive: number;
  noteThree: number;
  totalNote: number;
  parentsList: User[];
  currentPeriod: string;
  currentSubject: string;
  currentStudent: Student;
  studentsList: Student[];

  constructor(
    public authMdl: AuthModel,
    public studentMdl: StudentModel,
    public registerMdl: RegisterModel) {
  }

  ngOnInit(): void {
    this.totalNote = 0.0;
    this.studentsList = this.studentMdl.listStudents();
    this.parentsList = this.registerMdl.listUsers();
  }

  logout(): void {
    this.authMdl.logout();
  }

  averageNotes(): void {
    this.totalNote = (this.noteOne +
    this.noteTwo +
    this.noteThree +
    this.noteFour +
    this.noteFive) / 5;
  }

  saveNotesCurrentStudent(): void {
    this.period = {
      name: this.currentPeriod,
      notes: [this.noteOne, this.noteTwo, this.noteThree, this.noteFour, this.noteFive]
    };
    this.currentStudent.subjects.map(subject => {
      if (subject.name === this.currentSubject) {
        subject.periods.push(this.period);
      }
    });
    this.studentMdl.updateNotesCurrentStudent(this.currentStudent);
  }

}