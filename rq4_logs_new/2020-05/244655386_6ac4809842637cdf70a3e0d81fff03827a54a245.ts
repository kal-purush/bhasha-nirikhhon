import { RouterModule } from '@angular/router';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from 'src/app/shared/shared.module';
import {MatSidenavModule} from '@angular/material/sidenav';
import { MatDividerModule} from '@angular/material/divider';
import { FlexLayoutModule } from '@angular/flex-layout';
import { MatCardModule } from '@angular/material/card';
import {MatPaginatorModule} from '@angular/material/paginator';
import {MatTableModule, MatTableDataSource} from '@angular/material/table';
import {MatIconModule} from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatInputModule} from '@angular/material/input';
import {MatSnackBarModule} from '@angular/material/snack-bar';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import { FormsModule } from '@angular/forms';
import { MatTooltipModule } from '@angular/material/tooltip';
import { RevisionsComponent, CreationDialog, DescriptionDialog, TitleDialog, TimelineDialog, InfrastructureDialog, 
  DamageDialog, LocationDialog, TagDialog, IncidentDialog, AuthorDialog, ActorDialog, SectionDialog, HeaderComponent, DeletionDialog } from 'src/app/modules/revisions/revisions.component';
import { MatSortModule } from '@angular/material/sort';
import { MatDialogModule } from '@angular/material/dialog';
import {MatChipsModule} from '@angular/material/chips';


@NgModule({
  declarations: [
    RevisionsComponent,
    CreationDialog,
    DescriptionDialog,
    TitleDialog,
    TimelineDialog,
    InfrastructureDialog,
    DamageDialog,
    LocationDialog,
    TagDialog,
    IncidentDialog,
    AuthorDialog,
    ActorDialog,
    SectionDialog,
    HeaderComponent,
    DeletionDialog
  ],
  imports: [
    CommonModule,
    FormsModule,
    RouterModule,
    SharedModule,
    MatSidenavModule,
    MatDividerModule,
    MatCardModule,
    MatPaginatorModule,
    MatTableModule,
    MatIconModule,
    MatButtonModule,
    MatInputModule,
    MatFormFieldModule,
    MatSnackBarModule,
    MatCheckboxModule,
    MatTooltipModule,
    MatSortModule,
    FlexLayoutModule,
    MatDialogModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatChipsModule,
  ],
  providers: [
  ]
})
export class RevisionsModule { }