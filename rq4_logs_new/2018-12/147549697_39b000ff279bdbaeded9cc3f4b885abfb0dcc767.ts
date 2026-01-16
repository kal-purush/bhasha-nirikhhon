import { Component, OnInit } from '@angular/core';
import { ServerService } from '../../server.service';
import {MatSnackBar} from '@angular/material';
@Component({
  selector: 'app-add-user',
  templateUrl: './add-user.component.html',
  styleUrls: ['./add-user.component.scss']
})
export class AddUserComponent implements OnInit {
user: Object
admin: Boolean
archived: Boolean
first_name: string
surname: string
email: string
avatar: Object
avatars: Object
avatarId: number
avatarUrl:string
public images = [{
    name: 'Hase',
    id: 1,
    url: '../../../assets/images/avatar/rabbit-avatar.jpg',
    choose: false
  },{
    name: 'Bär',
    id: 2,
    url: '../../../assets/images/avatar/baer-avatar.jpg',
    choose: false
  },{
    name: 'Biber',
    id: 3,
    url:'../../../assets/images/avatar/biber-avatar.jpg',
    choose: false
  },{
    name: 'Eule',
    id: 4,
    url: '../../../assets/images/avatar/eule-avatar.jpg',
    choose: true
  }, {
    name: 'Fuchs',
    id: 5,
    url: '../../../assets/images/avatar/fuchs-avatar.jpg',
    choose: false,
  },{
    name: 'Koala',
    id: 6,
    url: '../../../assets/images/avatar/koala-avatar.jpg',
    choose: false,
  },{
    name: 'Luchs',
    id: 7,
    url: '../../../assets/images/avatar/luchs-avatar.jpg',
    choose: false
  },{
    name: 'Wolf',
    id: 8,
    url:'../../../assets/images/avatar/wolf-avatar.jpg',
    choose: false
  }]

  constructor(private serverService: ServerService, public snackBar: MatSnackBar) { }

  message = this.first_name + this.surname + 'wurde hinzugefügt';
  action = 'Ok';
  openSnackBar() {
      this.snackBar.open(this.message, this.action, {
        duration: 5000,
      });
    }

chooseAvatar(avatarId){
    avatarId= this.avatarId
    console.log(this.avatarId)
    console.log(this.avatarUrl)
}
    
  onSave(){
    let nextButton = document.querySelector('.next')
    let prevButton = document.querySelector('.prev')
    let wrapper = document.querySelector('.wrapper')
   
    this.user={
      admin: this.admin,
      archived: this.archived,
      first_name: this.first_name,
      surname: this.surname,
      email: this.email,
      'avatar': {
        id: this.chooseAvatar(this.avatarId),
        url: this.avatarUrl
      }
    }
    console.log(this.first_name)
    
    
       
        this.serverService.add(this.user, 'http://localhost:9000/api/users').subscribe(
            (response)=> console.log(response),
        )
        this.openSnackBar()
        // wrapper.setAttribute("style", "transform: translate( 0px)" );
       
            // prevButton.classList.add('hidden')
            // if(nextButton.classList.contains('hidden')){
            //     nextButton.classList.remove('hidden')
            //   }
} 

getAvatars(){
    this.serverService.getAll('http://localhost:9000/api/avatars').subscribe(
        (response)=> {
          this.avatars = response.json();
        },
        (error) => console.log(error)
      )

}
  ngOnInit() {
      this.getAvatars()
  }

}