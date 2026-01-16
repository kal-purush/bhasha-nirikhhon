// ?? code ??
import {Component, EventEmitter } from 'angular2/core';
import { RouterLink, RouteParams } from 'angular2/router';
import { Student } from '../models/student';
import { StudentFormatter } from '../pipes/studentFormatter';
import { StudentService } from '../services/studentservice';

@Component({
	selector:'student-card',
	inputs:['student'],
	pipes:[StudentFormatter],
	template:`
	<div class="dialog light-primary-color text-primary-color"><div [ngClass]="{defaultPrimaryColor:true}"> 
	{{ student.firstname }} {{ student.lastname }}</div>
	<p> Address: {{ student.address }} <br/> Phone: {{ student.phone }} </p></div>
	`,
	styles:[`
	.dialog { border:1px solid black;};
	.header { margin-top:0px;}
	`]
})
export class StudentCard
{
	student:Student;
	constructor(studentservice:StudentService/* ??code ?? */){
		/* ?? code ?? */
	}
}	