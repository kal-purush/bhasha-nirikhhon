import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';

import { AngularFireModule } from 'angularfire2';
import { AngularFireDatabaseModule } from 'angularfire2/database';

import { PaginationModule } from 'ngx-bootstrap';

import { AppComponent } from './app.component';
import { NavComponent } from './nav/nav.component';
import { MailingListComponent } from './mailing-list/mailing-list.component';
import { RecipeListComponent } from './recipe-list/recipe-list.component';
import { TimeSincePipe } from './shared/time-since/time-since.pipe';
import { RootDomainPipe } from './shared/root-domain/root-domain.pipe';
import { SearchPipe } from './shared/search/search.pipe';
import { RangePipe } from './shared/range/range.pipe';
import { CapitalizePipe } from './shared/capitalize/capitalize.pipe';
import { ArraySortPipe } from './shared/array-sort/array-sort.pipe';

const config = {
  apiKey: 'AIzaSyCglK8ZkZfL6QWWyEZf26z1BMPYFQM6wTw',
  authDomain: 'ketohub.firebaseapp.com',
  databaseURL: 'https://ketohub.firebaseio.com',
  projectId: 'ketohub',
  storageBucket: 'ketohub.appspot.com',
  messagingSenderId: '1012588055483'
};

@NgModule({
  declarations: [
    AppComponent,
    NavComponent,
    MailingListComponent,
    RecipeListComponent,
    TimeSincePipe,
    RootDomainPipe,
    SearchPipe,
    RangePipe,
    CapitalizePipe,
    ArraySortPipe
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,

    AngularFireModule.initializeApp(config),
    AngularFireDatabaseModule,

    PaginationModule.forRoot()
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }