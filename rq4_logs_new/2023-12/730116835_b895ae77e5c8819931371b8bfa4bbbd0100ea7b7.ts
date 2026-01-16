import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RegisterComponent } from './components/register/register.component';
import { LoginComponent } from './components/login/login.component';
import { FooterComponent } from '../../shared/components/footer/footer.component';
import { SharedModule } from '../../shared/shared.module';
import { AuthHeaderComponent } from './components/auth-header/auth-header.component';
import { AuthFooterComponent } from './components/auth-footer/auth-footer.component';
import { FormsModule } from '@angular/forms';

@NgModule({
  declarations: [
    RegisterComponent,
    LoginComponent,
    AuthHeaderComponent,
    AuthFooterComponent,
  ],
  imports: [CommonModule, SharedModule, FormsModule],
})
export class AuthenticationModule {}