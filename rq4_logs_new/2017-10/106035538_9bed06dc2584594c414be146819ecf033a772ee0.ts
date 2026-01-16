import { NgModule }             from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

 //about-us imports
 import { AboutUsComponent }   from '../about-us/about-us.component';
 import { HomeComponent }   from '../home/home.component';
 // completed imports
 import { CompletedComponent }   from '../completed/completed.component';
 //contact-us imports
 import { ContactUsComponent }   from '../contact-us/contact-us.component';
 //details imports
 import { DetailsComponent }   from '../details/details.component';
 //hike imports
 import { HikeComponent }   from '../hike/hike.component';
 //login imports
 import { LoginComponent }   from '../login/login.component';
 //profile imports
 import { ProfileComponent }   from '../profile/profile.component';
 //signup imports
 import { SignupComponent }   from '../signup/signup.component';
 //wishlist imports
 import { WishlistComponent }   from '../wishlist/wishlist.component';

 
 const routes: Routes = [
   { path: '', redirectTo: '/home', pathMatch: 'full' },
   { path: 'home',  component: HomeComponent },
   // about-us routes
   { path: 'about-us',  component: AboutUsComponent },
   // completed routes
   { path: 'completed',  component: CompletedComponent },
   // contact-us routes
   { path: 'contact-us',  component: ContactUsComponent },
   // details routes
   { path: 'details',  component: DetailsComponent },
   // hike routes
   { path: 'routes',  component: HikeComponent },
   // login routes
   { path: 'login',  component: LoginComponent },
   // signup routes
   { path: 'signup',  component: SignupComponent },
   // wishlist routes
   { path: 'wishlist',  component: WishlistComponent },
   
 ];
  
 @NgModule({
   imports: [ RouterModule.forRoot(routes) ],
   exports: [ RouterModule ]
 })
 export class AppRoutingModule {}
 