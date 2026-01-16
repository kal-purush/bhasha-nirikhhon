import { Component, OnInit, Input } from "@angular/core";

@Component({
  selector: "event-thumbnail",
  template: `
    <mat-card class="event-card">
      <mat-card-header>
        <img
          mat-card-avatar
          src="../../assets/gdg_cloud_oc_small_circle_logo.png"
        />
        <mat-card-title
          ><strong>{{ item?.name }}</strong></mat-card-title
        >
        <mat-card-subtitle
          >{{ item?.local_date }} | {{ item?.local_time }}</mat-card-subtitle
        >
      </mat-card-header>
      <mat-divider></mat-divider>
      <mat-card-content>
        <p>{{ item?.description }}</p>
      </mat-card-content>
      <mat-divider></mat-divider>

      <mat-card-actions class="event-footer">
        <span
          >{{ item?.yes_rsvp_count }} of {{ item?.rsvp_limit }} attending </span
        ><button>
          RSVP
        </button></mat-card-actions
      >
    </mat-card>
  `,
  styles: [
    `
      .event-card {
        margin-top: 20px;
        max-width: 80%;
      }
      .event-card p {
        padding-top: 10px;
        text-align: left;
      }
      .event-footer span {
        padding: 10px;
      }
      .event-footer {
        text-align: right;
      }
    `
  ]
})
export class EventThumbnailComponent implements OnInit {
  @Input() item: any;
  constructor() {}

  ngOnInit() {}
}