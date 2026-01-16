import { Component, OnInit } from '@angular/core';
import Swal from 'sweetalert2';

@Component({
  selector: 'app-courses',
  templateUrl: './courses.component.html',
  styleUrls: ['./courses.component.css']
})
export class CoursesComponent implements OnInit {

  title = 'formationAngular';

  onUpdate = false;
  compteur:number = 0;
  constructor() { }

  ngOnInit() {
  }
  
    currentCourse = {
                      id:0,
                      title:"",
                      active:false,
                      favoris:false,
                      vote:{
                        like:0,
                        dislike:0
                      },
                      price:0,
                      date:new Date(),
                      description:""
                    };
                    
    courses = [{
                  id:1,
                  title:"Spring boot",
                  active:true,
                  favoris:false,
                  price:1.20,
                  date:new Date(),
                  vote:{
                    like:0,
                    dislike:0
                  },
                  description:"Lorem Ipsum  Lorem Ipsum HSkjds dezoke Lorem Ipsum HSkjds dezokeHSkjds dezoke"
                },
                {
                  id:2,
                  title:"Scala",
                  active:true,
                  favoris:false,
                  price:10.25,
                  date:new Date(),
                  vote:{
                    like:0,
                    dislike:0
                  },
                  description:"Lorem Ipsum  Lorem Ipsum HSkjds dezoke Lorem Ipsum HSkjds dezokeHSkjds dezoke"
  
                },
                {
                  id:3,
                  title:"Haskell",
                  active:false,
                  favoris:false,
                  price:11.25,
                  date:new Date(),
                  vote:{
                    like:0,
                    dislike:0
                  },
                  description:"Lorem Ipsum  Lorem Ipsum HSkjds dezoke Lorem Ipsum HSkjds dezokeHSkjds dezoke"
                },
                {
                  id:4,
                  title:"C/C++",
                  active:true,
                  favoris:false,
                  price:90.25,
                  date:new Date(),
                  vote:{
                    like:0,
                    dislike:0
                  },
                  description:"Lorem Ipsum  Lorem Ipsum HSkjds dezoke Lorem Ipsum HSkjds dezokeHSkjds dezoke"
                }
              ];
  
    addCourse(){
      this.courses.unshift(this.currentCourse);
      this.currentCourse={
        id:0,
        title:"",
        active:false,
        favoris:false,
        price:0,
        date:new Date(),
        vote:{
          like:0,
          dislike:0
        },
        description:"loremlorem Ipsum lorem  lorem Ipsum lorem  Ipsum lorem "
      };
    }
    deleteCourse(course){
  
      Swal({
        title: 'Are you sure?',
        text: 'You will not be able to recover this imaginary file!',
        type: 'warning',
        showCancelButton: true,
        confirmButtonText: 'Yes, delete it!',
        cancelButtonText: 'No, keep it'
      }).then((result) => {
        if (result.value) {
          Swal(
            'Deleted!',
            'Your imaginary file has been deleted.',
            'success'
          )
        } else if (result.dismiss === Swal.DismissReason.cancel) {
          Swal(
            'Cancelled',
            'Your imaginary file is safe :)',
            'error'
          )
        }
      })
       
        let index=this.courses.indexOf(course);
        this.courses.splice(index,1);
    }
    editCourse(course){
      this.currentCourse=course;
      this.onUpdate=true;
    }
    updateCourse(){
      this.onUpdate=false;
    }
    toggleButton(course){
      course.active=!course.active;
    }
    starOnOff(course){
      course.favoris=!course.favoris;
    }
    updateVoteLike(course,child){
        course.vote.like=child;
     }
     updateVoteDislike(course,child){
       course.vote.dislike=child;
     }
}