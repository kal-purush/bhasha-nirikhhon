import {Component, OnInit} from 'angular2/core';
import {Skill} from './../skills/skill';
import {SkillService} from './../skills/skill.service.ts';
import {DataProviderService} from "../util/data-provider.service";
import {Position, Education} from '../experience/experience'
import {Personal} from "../personal/personal";


@Component({
    selector: 'resume',
    templateUrl: 'app/components/overview/resume.component.html',
    directives: []
})
export class ResumeComponent implements OnInit {

    personal:Personal;
    positions:Position[] = [];
    skillSummary:Skill[] = [];
    educations:Education[] = [];

    constructor(private _skillService:SkillService,
                private _dataProviderService:DataProviderService) {
    }

    ngOnInit() {
        this._skillService.getSkills()
            .then(skills => this.skillSummary = skills.slice(0, 4));

        this._dataProviderService.getPositions()
            .then(positions => this.positions = positions);

        this._dataProviderService.getPersonal()
            .then(personal => this.personal = personal);

        this._dataProviderService.getEducation()
            .then(educations => this.educations = educations);
    }

}