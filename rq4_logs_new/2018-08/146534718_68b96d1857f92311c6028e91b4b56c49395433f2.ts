import { NgModule } from "@angular/core";
import {Routes,RouterModule} from '@angular/router';
import { ProjectSummaryComponent } from "./projectSummary/projectSummary.component";

const appRoutes:Routes=[
    {path:'', redirectTo:'/projectSummary', pathMatch:'full'}, 
    {path: 'projectSummary', component:ProjectSummaryComponent},


];

@NgModule({
    imports: [RouterModule.forRoot(appRoutes
        // ,{onSameUrlNavigation: 'reload'}
    )],
    exports:[RouterModule]
})
export class AppRoutingModule{

}