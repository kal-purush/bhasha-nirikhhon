import {Component, OnInit} from 'angular2/core';
import {StudentService} from './student.service';
import {GlobalVarsService} from '../global-vars.service';
import {Student} from './student';

@Component({
    selector: 'student-details',
    template: `
            <h3>Hello From student details </h3>
            <ul>
                <li *ngFor = "#student of students">
                    {{student.ime}}
                </li>
           </ul>
            
                
    `,
    providers: [StudentService]
})

export class StudentDetailsComponent implements OnInit{
    constructor (private _studentService: StudentService, private _gVS: GlobalVarsService){}
   
    public students: Student[];
    
    public getStud(){
        this._studentService.getStudents().subscribe(
            data =>{this.students = data},
            err =>console.error(err),
            ()=>console.log('done loading students')  
        );
    }
    /*public getStudents(){
       
        this._examService.getStudents().subscribe(
      // the first argument is a function which runs on success
      data => { this.students = data},
      // the second argument is a function which runs on error
      err => console.error(err),
      // the third argument is a function which runs on completion
      () => console.log('done loading foods')
        );
    }
    */
    ngOnInit(){
        this.getStud();
    }
}