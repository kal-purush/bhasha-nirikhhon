import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {FormsModule, ReactiveFormsModule}   from '@angular/forms';
import {HttpModule} from '@angular/http';
import {RouterModule} from '@angular/router';
import {CaseService}       from './cases/case.service';
import {CasefileService}   from './casefiles/casefile.service';
import {CasetagService}       from './casetags/casetag.service';
import {CommentService}       from './comments/comment.service';
import {DeterminationService}       from './determinations/determination.service';
import {FieldofficeService}       from './fieldoffices/fieldoffice.service';
import {ProhibitiondateService}       from './prohibitiondates/prohibitiondate.service';
import {SystemmapService}       from './systemmaps/systemmap.service';
import {SystemunitService}       from './systemunits/systemunit.service';
import {UserService}       from './users/user.service';
import {PropertyService}   from './properties/property.service';
import {RequesterService}  from './requesters/requester.service';
import {AuthenticationService} from './authentication/authentication.service';
import {AppComponent}   from './app.component';
import {NavbarComponent}   from './navbar.component';
import {LoginComponent}   from './authentication/login.component';
import {TagListComponent}   from './tags/tag-list.component';
import {TagDetailComponent}   from './tags/tag-detail.component';
import {WorkbenchComponent}   from './workbench/workbench.component';
import {WorkbenchListComponent}   from './workbench/workbench-list.component';
import {WorkbenchGridComponent}   from './workbench/workbench-grid.component';
import {WorkbenchFilterComponent}   from './workbench/workbench-filter.component';
import {WorkbenchDetailComponent}   from './workbench/workbench-detail.component';
import {ReportComponent}   from './reports/report.component';
import {ReportListComponent}   from './reports/report-list.component';
import {ReportGridComponent}   from './reports/report-grid.component';
import {ReportCasesByUnitComponent}   from './reports/report-cases-by-unit.component';
import {routing, appRoutingProviders} from './app.routing';

@NgModule({
    imports: [
            routing, BrowserModule, FormsModule, ReactiveFormsModule, RouterModule, HttpModule
        ],
    declarations: [
        AppComponent, NavbarComponent, LoginComponent, TagListComponent, TagDetailComponent,
        WorkbenchComponent, WorkbenchListComponent, WorkbenchGridComponent, WorkbenchFilterComponent, WorkbenchDetailComponent,
        ReportComponent, ReportListComponent, ReportGridComponent, ReportCasesByUnitComponent
    ],
    providers: [
        appRoutingProviders, CaseService, CasefileService, CasetagService, PropertyService, RequesterService, AuthenticationService,
        CommentService, DeterminationService, FieldofficeService, ProhibitiondateService, SystemmapService, SystemunitService, UserService
    ],
    bootstrap: [AppComponent]
})
export class AppModule {}