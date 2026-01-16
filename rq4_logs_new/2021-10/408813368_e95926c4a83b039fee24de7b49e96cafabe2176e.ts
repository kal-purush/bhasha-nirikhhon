import {
  COLLECTION_DISPLAY_NAME,
  COLLECTION_NAME,
  DOMAIN_DISPLAY_NAME,
} from './injection-tokens';
import { AngularFirestore } from '@angular/fire/firestore';
import {
  AfterContentInit,
  Component,
  ContentChildren,
  Inject,
  Input,
  OnInit,
  QueryList,
  ViewChild,
} from '@angular/core';
import { MatColumnDef, MatTable } from '@angular/material/table';

@Component({
  selector: 'app-table',
  template: `
    <mat-card class="p-0  rounded-xl">
      <mat-card class="rounded-t-xl mat-elevation-z0 ">
        <div class="flex justify-between items-center">
          <div class="text-gray-600 text-lg">{{ displayName }}</div>
          <div class="controls ">
            <button
              mat-button
              class="bg-blue-500  rounded-lg text-white"
              appFormCreateTrigger
            >
              Add {{ domainName }}
            </button>
          </div>
        </div>
      </mat-card>

      <table
        mat-table
        [dataSource]="items"
        class="rounded-none   mat-elevation-z0 w-full"
      >
        <ng-content></ng-content>

        <tr mat-header-row *matHeaderRowDef="columns"></tr>
        <tr mat-row *matRowDef="let row; columns: columns"></tr>
      </table>
      <mat-paginator
        class="rounded-b-xl"
        [length]="100"
        [pageSize]="10"
        [pageSizeOptions]="[1, 5, 10, 25, 100]"
        aria-label="Select page"
      >
      </mat-paginator>
    </mat-card>
  `,
  styleUrls: ['./table.component.scss'],
})
export class TableComponent implements OnInit, AfterContentInit {
  @Input() columns: string[] = [];

  @Input() orderByField: string | undefined = undefined;

  @Input() orderByDirection: 'desc' | 'asc' = 'asc';

  @ContentChildren(MatColumnDef) columnDefs!: QueryList<MatColumnDef>;

  @ViewChild(MatTable, { static: true }) table!: MatTable<any>;

  items = this.firestore
    .collection(this.collectionName, (ref) => {
      if (this.orderByField) {
        ref.orderBy(this.orderByField, this.orderByDirection);
      }

      return ref;
    })
    .valueChanges({ idField: 'id' });

  constructor(
    private firestore: AngularFirestore,
    @Inject(COLLECTION_NAME) private collectionName: string,

    @Inject(COLLECTION_DISPLAY_NAME) public displayName: string,

    @Inject(DOMAIN_DISPLAY_NAME) public domainName: string
  ) {}
  ngAfterContentInit(): void {
    this.columnDefs.forEach((columnDef) => this.table.addColumnDef(columnDef));
  }

  ngOnInit(): void {}
}