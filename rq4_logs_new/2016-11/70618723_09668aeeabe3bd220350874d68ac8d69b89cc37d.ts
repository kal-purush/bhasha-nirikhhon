/**
 * Created by Peter on 11/7/16.
 */

import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import {AssessmentFormService} from './assessment-form.service';

@Component({
    moduleId: module.id,
    selector: 'my-assessment-form',
    templateUrl: 'assessment-form.component.html',
    providers: [AssessmentFormService]
})

export class AssessmentFormComponent implements OnInit{
    constructor(private assessmentFormService: AssessmentFormService, private router: Router, private route: ActivatedRoute) {}

    //Member fields
    questions: string[];
    answers: Number[];

    //Member functions
    getQuestions(): void {
        let questions = this.assessmentFormService.getQuestions();
        this.questions = questions;
    }
    submitForm(): void{
        console.log(this.answers);
        if(this.answers.indexOf(-1)!=-1){
            alert('Please select an answer for every questions');
            return;
        }
        let id = this.assessmentFormService.sendAnswers(this.answers);
        this.router.navigate(['../'+id], {relativeTo: this.route});
    }

    //Lifecycle functions
    ngOnInit(): void {
        this.getQuestions();
        this.answers = [ ];
        for (let question in this.questions){
            this.answers.push(-1);
        }
    }
}