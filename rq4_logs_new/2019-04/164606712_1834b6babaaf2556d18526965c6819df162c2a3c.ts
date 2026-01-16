import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { MenuComponent } from './components/menu/menu.component';
import { DataReviewComponent } from './components/data-review/data-review.component';
import { RecordListComponent } from './components/record-list/record-list.component';
import { DataManagementComponent } from './data-management.component';
import { RecordDetailResolverService } from 'src/app/user/modules/records/services/record-detail-resolver.service';
import { BatchSubmitComponent } from './components/batch-submit/batch-submit.component';

const routes: Routes = [
  {
    path: 'data',
    component: DataManagementComponent,
    children: [
      {
        path: 'records',
        children: [
          {
            path: ':id',
            resolve: {
              record: RecordDetailResolverService,
            },
            component: DataReviewComponent,
          },
          {
            path: '',
            component: RecordListComponent,
          }
        ]
      },
      {
        path: 'batch-submit',
        component: BatchSubmitComponent,
      },
      {
        path: '',
        component: MenuComponent,
      }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DataManagementRoutingModule { }