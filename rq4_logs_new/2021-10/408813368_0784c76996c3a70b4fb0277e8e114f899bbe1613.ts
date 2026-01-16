import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-engagement-table',
  template: `
    <app-table [columns]="columns" matSort #matSort="matSort" [sort]="matSort">
      <ng-container matColumnDef="id">
        <th mat-header-cell *matHeaderCellDef>Id</th>
        <td mat-cell *matCellDef="let element">{{ element.id }}</td>
      </ng-container>
      <ng-container matColumnDef="patient">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Patient</th>
        <td mat-cell *matCellDef="let element">
          {{
            renderFullName(
              element.patient.firstName,
              element.patient.middleName,
              element.patient.lastName
            )
          }}
        </td>
      </ng-container>

      <ng-container matColumnDef="type">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Type</th>
        <td mat-cell *matCellDef="let element">
          {{ element.engagementType.name }}
        </td>
      </ng-container>

      <ng-container matColumnDef="bloodPressure">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>
          Blood Pressure
        </th>
        <td mat-cell *matCellDef="let element">
          {{ element.systolicBloodPressure }}/{{
            element.diastolicBloodPressure
          }}
        </td>
      </ng-container>

      <ng-container matColumnDef="height">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Height</th>
        <td mat-cell *matCellDef="let element">{{ element.height }}</td>
      </ng-container>

      <ng-container matColumnDef="weight">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Weight</th>
        <td mat-cell *matCellDef="let element">
          {{ element.weight }}
        </td>
      </ng-container>

      <ng-container matColumnDef="temperature">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Temperature</th>
        <td mat-cell *matCellDef="let element">{{ element.temperature }}</td>
      </ng-container>

      <ng-container matColumnDef="story">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Story</th>
        <td mat-cell *matCellDef="let element">
          {{ element.story }}
        </td>
      </ng-container>

      <ng-container matColumnDef="createdAt">
        <th mat-header-cell *matHeaderCellDef mat-sort-header>Created At</th>
        <td mat-cell *matCellDef="let element">
          {{ element.createdAt.toDate() | date }}
        </td>
      </ng-container>
    </app-table>
  `,
})
export class EngagementTableComponent implements OnInit {
  columns = [
    'id',
    'patient',
    'type',
    'bloodPressure',
    'weight',
    'height',
    'story',
    'temperature',
    'createdAt',
  ];

  constructor() {}

  ngOnInit(): void {}

  renderFullName(
    firstName: string,
    middleName: string,
    lastName: string
  ): string {
    return `${firstName} ${middleName} ${lastName}`;
  }
}