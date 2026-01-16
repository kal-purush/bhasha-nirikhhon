import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { LoginComponent } from './login/login.component';
import { ChatComponent } from './chat/chat.component';
import { RegisterComponent } from './register/register.component';
import { VideoChatComponent } from './video-chat/video-chat.component';

const routes: Routes = [
  {
    path: '',
    component: LoginComponent,
    title: 'Login Page'
  },
  {
    path: '\chat',
    component: ChatComponent,
    title: 'Main Page'
  },
  {
    path: '\register',
    component: RegisterComponent,
    title: 'Sign In Page'
  },
  {
    path: '\video',
    component: VideoChatComponent,
    title: 'Video Chat Page'
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }