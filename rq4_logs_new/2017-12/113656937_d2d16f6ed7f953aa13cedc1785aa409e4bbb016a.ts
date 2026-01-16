import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {UserService} from "../../service/user.service";
import {Course} from "../../entity/course";
import {Location} from '@angular/common';
import {CourseService} from '../../service/course.service';
import {SkillsService} from '../../service/skills.service';
import {KnowledgeService} from '../../service/knowledge.service';
import {ProgramService} from '../../service/program.service';
import {Program} from '../../entity/program';
import {AnalyzeResult} from '../../entity/AnalyzeResult';

@Component({
  selector: 'program-analyze',
  templateUrl: './analyze.component.html',
  styleUrls: ['./analyze.component.css']
})
export class AnalyzeComponent implements OnInit {
  program: Program;
  analyzeResult: AnalyzeResult;

  constructor(private userService: UserService,
              private router: Router,
              private location: Location,
              private route: ActivatedRoute,
              private courseService: CourseService,
              private skillsService: SkillsService,
              private knowledgeService: KnowledgeService,
              private programService: ProgramService) {
  }

  ngOnInit() {
    let id = +this.route.snapshot.paramMap.get('id');

    this.programService.getProgram(id).subscribe(program=>{
      this.program = program;
    });

    this.programService.analyze(id).subscribe( analyzeResult =>{
      this.analyzeResult = analyzeResult;
    });
  }

  goBack(){
    this.location.back();
  }
}