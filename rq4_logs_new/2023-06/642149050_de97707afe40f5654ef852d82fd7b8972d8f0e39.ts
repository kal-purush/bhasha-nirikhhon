import { Component, Input } from '@angular/core';
import { Post } from 'src/app/interfaces/post';
import { User } from 'src/app/interfaces/user';
import { UserService } from 'src/app/services/user.service';

@Component({
  selector: 'app-slider-element',
  templateUrl: './slider-element.component.html',
  styleUrls: ['./slider-element.component.css']
})
export class SliderElementComponent {
@Input() post!: Post;
user!: User | undefined;

constructor(private userService : UserService) {
  this.userService.getUserByUsername(this.post.uploader)
  .subscribe(user => this.user = user);
 }

}