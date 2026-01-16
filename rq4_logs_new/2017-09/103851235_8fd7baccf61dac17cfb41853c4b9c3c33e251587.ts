import {Component, AfterViewInit, Input, OnDestroy, ElementRef, OnInit, ComponentFactoryResolver} from '@angular/core';
import {BaseService} from './../base-component/base.service';
import {Http, Headers, Response} from '@angular/http';
import {Observable} from 'rxjs/Rx';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';

import { HttpModule }    from '@angular/http';

//import { ROUTER_DIRECTIVES } from '@angular/router';
//import {Http} from '@angular/http';
import {BigDeltaViewComponent} from './../../components/andon-component/big-delta-view/big-delta-view.component';
import {HourlyViewComponent} from './../../components/andon-component/hourly-view/hourly-view.component';
import {ShiftViewComponent} from './../../components/andon-component/shift-view/shift-view.component';
import {ShiftTimeViewComponent} from './../../components/andon-component/shift-time-view/shift-time-view.component';
import {HeaderUIAdminComponent} from './../../components/layout-component/header-ui-admin/header-ui-admin.component';
//import {PdfViewerWrapperComponent} from './viewComponents/pdf-viewer/pdf-viewer-wrapper.component';
import {ClassOrientedManufacturing} from './../../components/class-oriented-manufacturing-component/class-oriented-manufacturing.component';
import {AppStoreComponent} from './../../components/layout-component/app-store/app-store-view.component';
import {SpinnerComponent} from './../../components/misc-component/spinner-view/spinner-view.component';
import {ServiceUnavailableComponent} from './../../components/misc-component/service-unavailable-component/service-unavailable.component';

import { ContextMenuComponent } from './../../components/layout-component/layout-ui-view/contextMenu.component';
import { ContextMenuService } from './../../components/layout-component/layout-ui-view/contextMenu.service';
import { TabComponent } from './../../components/misc-component/tabs/tab.component';
import { TabsComponent } from './../../components/misc-component/tabs/tabs.component';
import {DropdownComponent} from './../../components/misc-component/drop-down-view/drop-down.component';
import {SettingsComponent} from '../../components/settings.component/settings.component';
declare var textFit: any;

@Component({
    selector: 'admin-view',
    moduleId: module.id,
    templateUrl: 'admin.component.html',
    providers: [HeaderUIAdminComponent,HourlyViewComponent,ShiftViewComponent,ShiftTimeViewComponent,ClassOrientedManufacturing,
                AppStoreComponent,ContextMenuComponent,ContextMenuService,
                TabComponent,TabsComponent,SpinnerComponent,ServiceUnavailableComponent,SettingsComponent],
})
export class AdminComponent {
} 