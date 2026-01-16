import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {EventsListComponent} from './events/events-list.component';
import {EventThumbnailComponent} from './events/event-thumbnail.component';
import {EventsAppComponent} from './events-app.component';

@NgModule({
    imports: [
        BrowserModule
    ],
    declarations: [
        EventsAppComponent, EventsListComponent, EventThumbnailComponent
    ],
    bootstrap: [
        EventsAppComponent
    ]
})
export class AppModule{
    
}