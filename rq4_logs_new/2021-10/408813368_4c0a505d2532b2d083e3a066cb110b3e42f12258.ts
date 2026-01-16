import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-user-table',
  template: `
    <app-table
      [columns]="displayedColumns"
      orderByField="createdAt"
      orderByDirection="desc"
    >
      <ng-container matColumnDef="id">
        <th mat-header-cell *matHeaderCellDef>Id</th>
        <td mat-cell *matCellDef="let element">{{ element?.id }}</td>
      </ng-container>
      <ng-container matColumnDef="email">
        <th mat-header-cell *matHeaderCellDef>E-mail</th>
        <td mat-cell *matCellDef="let element">{{ element?.email }}</td>
      </ng-container>

      <ng-container matColumnDef="createdAt">
        <th mat-header-cell *matHeaderCellDef>Created At</th>
        <td mat-cell *matCellDef="let element">{{ element?.createdAt }}</td>
      </ng-container>

      <ng-container matColumnDef="options">
        <th mat-header-cell *matHeaderCellDef></th>
        <td mat-cell *matCellDef="let element">
          <div class="flex justify-end">
            <button mat-icon-button [matMenuTriggerFor]="menu">
              <mat-icon>more_vert</mat-icon>
            </button>
            <mat-menu #menu="matMenu">
              <button mat-menu-item appDeleteTrigger [id]="element.id!">
                Delete
              </button>
            </mat-menu>
          </div>
        </td>
      </ng-container>
    </app-table>
  `,
})
export class UserTableComponent implements OnInit {
  displayedColumns = ['id', 'email', 'createdAt', 'options'];
  constructor() {}

  ngOnInit(): void {}
}