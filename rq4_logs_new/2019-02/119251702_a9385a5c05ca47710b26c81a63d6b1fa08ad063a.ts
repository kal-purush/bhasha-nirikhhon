import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {
  MatButtonModule, MatCardModule, MatCheckboxModule, MatDialogModule, MatIconModule, MatInputModule,
  MatPaginatorModule, MatSelectModule,
  MatSnackBarModule, MatTabsModule
} from '@angular/material';
import {FlexLayoutModule} from '@angular/flex-layout';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {SharedModule} from '../../shared/shared.module';
import {MarkdownModule} from 'angular2-markdown';
import {AppTrackListComponent} from './app-tracklist.component';
import {AppTrackListRouting} from './app-tracklist.routing';

@NgModule({
  declarations: [
    AppTrackListComponent,
  ],
  imports: [
    AppTrackListRouting,
    CommonModule,
    MatButtonModule,
    MatIconModule,
    FlexLayoutModule,
    FormsModule,
    ReactiveFormsModule,
    FlexLayoutModule,
    MatCardModule,
    MatInputModule,
    MatButtonModule,
    MatSnackBarModule,
    MatIconModule,
    MatDialogModule,
    MatSelectModule,
    MatTabsModule,
    MatPaginatorModule,
    MatSelectModule,
    MatTabsModule,
    MatCheckboxModule,
    SharedModule,
    MarkdownModule.forRoot(),
  ],
})
export class AppTracklistModule {
}