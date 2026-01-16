import { Component, OnInit, Input } from "@angular/core";

@Component({
  selector: "event-jumbotron",
  template: `
    <mat-jumbotron
      class="jumbotron-card"
      imgURL="../../assets/gdg_cloud_oc_small_logo.png"
    >
      <mat-jumbotron-content>
        <h1>{{ event?.name }}</h1>

        <h3>
          {{ this.convertDate(event?.local_date) }} |
          {{ this.convertTime(event?.local_time) }}
        </h3>

        <p [innerHtml]="event?.description"></p>

        <mat-card-actions class="jumbotron-footer">
          <span
            >{{ event?.yes_rsvp_count }} of {{ event?.rsvp_limit }} attending
          </span>
          <a mat-button href="{{ event?.link }}" color="primary">RSVP</a>
        </mat-card-actions>
      </mat-jumbotron-content>
    </mat-jumbotron>
  `,
  styles: [
    `
      .jumbotron-card {
        margin: 20px auto 0 auto;
        padding: 10px;
        font-size: medium;
      }
      .jumbotron-card p {
        padding-top: 10px;
        text-align: left;
      }
      .jumbotron-footer span {
        padding: 10px;
      }
      .jumbotron-footer {
        text-align: right;
      }
    `
  ]
})
export class EventJumbotronComponent implements OnInit {
  @Input() event: any;
  constructor() {}

  ngOnInit() {}

  convertDate(d) {
    const a = d.slice(5);
    const b = d.slice(0, 4);
    return a + "-" + b;
  }

  convertTime(t) {
    const h = t.slice(0, 2);
    const s = t.slice(3);
    if (h >= 12) {
      return h - 12 + ":" + s + " PM";
    }
  }
}