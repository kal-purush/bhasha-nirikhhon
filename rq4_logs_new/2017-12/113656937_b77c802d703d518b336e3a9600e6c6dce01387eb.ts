import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {UserService} from "../../service/user.service";
import {Course} from "../../entity/course";
import {Location} from '@angular/common';

@Component({
  selector: 'app-course',
  templateUrl: './course.component.html',
  styleUrls: ['./course.component.css']
})
export class CourseComponent implements OnInit {
  course: Course;
  creatingNew: boolean;
  attestationForms: string[];
  basicEducationProgramId: number;

  constructor(private userService: UserService, private router: Router, private location: Location, private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.basicEducationProgramId = +this.route.snapshot.paramMap.get('programId');
    let id = this.route.snapshot.paramMap.get('id');
    this.creatingNew = (id == 'new');

    console.log(this.route.snapshot.paramMap);
    console.log(this.creatingNew);

    if(this.creatingNew){
      this.course = new Course();
    }else{
      // Load from server
    }
  }

  save(){

  }

  create(){

  }

  cancel(){
    this.location.back();
  }
}