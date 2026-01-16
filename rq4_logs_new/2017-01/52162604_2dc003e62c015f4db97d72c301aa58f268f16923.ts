import {NgModule}      from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {FormsModule} from "@angular/forms";
import {HttpModule} from "@angular/http";
import {AppComponent}   from './components/app.component';
import {ConferenceComponent} from "./components/experience/conference.component";
import {EducationComponent} from "./components/experience/education.component";
import {ExperienceComponent} from "./components/experience/experience.component";
import {PositionComponent} from "./components/experience/position.component";
import {RouterModule, Routes} from "@angular/router";
import {OverviewComponent} from "./components/overview/overview.component";
import {SkillsComponent} from "./components/skills/skills.component";
import {SkillDetailComponent} from "./components/skills/skill-detail.component";
import {PersonalComponent} from "./components/personal/personal.component";
import {ResumeComponent} from "./components/overview/resume.component";
import {ContactComponent} from "./components/personal/contact.component";
import {SkillChartComponent} from "./components/skills/skill-chart.component";

const appRoutes: Routes = [
    {path: 'overview', component: OverviewComponent},
    {path: 'skills', component: SkillsComponent},
    {path: 'skills/:name', component: SkillDetailComponent},
    {path: 'experience', component: ExperienceComponent},
    {path: 'personal', component: PersonalComponent},
    {path: 'resume', component: ResumeComponent},

    {path: '', redirectTo: '/overview', pathMatch: 'full'},
    // {path: '**', component: OverviewComponent}
];

@NgModule({
    imports: [BrowserModule, FormsModule, HttpModule, RouterModule.forRoot(appRoutes)],
    declarations: [
        AppComponent,
        ExperienceComponent,
        ConferenceComponent,
        EducationComponent,
        PositionComponent,
        OverviewComponent,
        SkillsComponent,
        SkillDetailComponent,
        SkillChartComponent,
        OverviewComponent,
        PersonalComponent,
        ResumeComponent,
        ContactComponent
    ],
    bootstrap: [AppComponent]
})
export class AppModule {
}