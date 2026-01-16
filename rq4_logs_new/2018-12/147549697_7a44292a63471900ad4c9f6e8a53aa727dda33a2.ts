import { Component, OnInit } from '@angular/core';
import {HttpClient} from '@angular/common/http'
import { ServerService } from '../../server.service';
import {MatSnackBar} from '@angular/material';


@Component({
    selector: 'app-add-todo',
    templateUrl: './add-todo.component.html',
    styleUrls: ['./add-todo.component.scss']
})
export class AddTodoComponent  implements OnInit {
    name:string
    data: Object
    todo:Object
    proj:Array<String>;
    title: string
    description:string
    selectedUser
    status: string
    u: Object
    projectID
    
    
    
    constructor(private serverService: ServerService, public snackBar: MatSnackBar) { 
        
    }
    message = this.name + 'wurde hinzugefügt';
    action = 'Ok';
    openSnackBar() {
        this.snackBar.open(this.message, this.action, {
            duration: 5000,
        });
    }
    
    getProject(){
        this.serverService.getAll('http://localhost:9000/api/projects').subscribe(
        (response)=> {
            
            this.data = response.json();
        },
        (error) => console.log(error) 
    )
}

bleh(id) {
    console.log(id)
    this.projectID = id
    this.serverService.getAll('http://localhost:9000/api/projects/' + id).subscribe((response) => {
    let proj = [];
    
    let project = response.json();
    console.log(project.todos)
    project.assigned_users.forEach((user) => {
        proj.push({
            "archived": user.archived,
            "_id": user._id,
            "first_name": user.first_name,
            "surname": user.surname
            
        });
    });
    
    
    this.proj = proj;
    console.log(proj)
    console.log(this.proj);
},
(error) => console.log(error)
)

}

getUser(id) {
    console.log(id)
    this.serverService.getAll('http://localhost:9000/api/users/' + id).subscribe((response) => {
    let proj = [];
    
    this.u = response.json();
  },
  (error) => console.log(error)
  )
  
  }

onSave(){
    let nextButton = document.querySelector('.next')
    let prevButton = document.querySelector('.prev')
    let wrapper = document.querySelector('.wrapper')
    this.status = "0"
    
    this.todo={
        status: this.status,
        project: this.projectID,
        briefing:{
            title: this.title,
            description: this.description
        },
        assigned_users: [this.u]
    }
    console.log(typeof this.status )
    this.serverService.add(this.todo, 'http://localhost:9000/api/todos').subscribe(
    (response)=> console.log(response),
)
this.openSnackBar()
}  
checkValid(){
    let invalid: Boolean
    let error = document.querySelector('.error-message')
    let input = document.querySelectorAll('mat-form-field')
    
    
    for(let i = 0; i < input.length; i++){
        console.log(input[i])
        if(input[i].classList.contains('ng-invalid')){
            console.log(input[i])
            invalid = true;
        }
    }
    if(!invalid){
        this.onSave()
    }else{
        error.classList.remove('hidden')
        
    }
}


ngOnInit() {
    this.getProject()
    
}

}