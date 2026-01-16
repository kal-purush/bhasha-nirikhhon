import { AuthGuard } from './shared/auth.guard';
import { AuthService } from './shared/auth.service';
import { NgModule } from "@angular/core";
import { CommonModule } from "@angular/common";
import { RouterModule, Routes } from "@angular/router";
import { RegisterComponent } from "./register/register.component";
import { LoginComponent } from "./login/login.component";
import { AuthComponent } from "./auth.component";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HTTP_INTERCEPTORS } from '@angular/common/http';
import { AuthInterceptor } from './shared/auth.interceptor';
const authRoutes: Routes = [
  { path: "login", component: LoginComponent, canActivate: [AuthGuard] },
  { path: "register", component: RegisterComponent, canActivate: [AuthGuard]}
];
@NgModule({
  imports: [CommonModule, RouterModule.forChild(authRoutes), FormsModule, ReactiveFormsModule],
  declarations: [RegisterComponent, LoginComponent, AuthComponent],
  providers: [AuthService, AuthGuard,{provide: HTTP_INTERCEPTORS, useClass: AuthInterceptor, multi: true}]
})
export class AuthModule {}