import {Component}           from 'angular2/core';
import {TimeVsPix}           from './time-vs-pix';
import {TimespanComponent}   from './timespan.component';
import {ResponseData, ResourceSpec}
                             from './resource-time-block';

@Component({
  selector: 'timespans-data',
  // Invoke as:
  //   <timespans-data [resource_specs]="resource_specs"
  //                   [timespans_hash]="timespans_hash"
  //                   [time_pix]="time_pix"></timespans-data>
  template: `
    <div *ngFor="#res_tag of resourceTags()"
         class="rsrcRow {{ kindFromTag(res_tag) }}row">

      <timespan [time_blocks]="timespans_hash[res_tag]"
                [time_pix]="time_pix"></timespan>

    </div>
  `,
  styleUrls: ['app/scheduled-resource.css'],
  directives: [TimespanComponent],
  inputs: ['resource_specs', 'timespans_hash', 'time_pix'],
  providers: []
})

export class TimespansDataComponent {
  public resource_specs: ResourceSpec[];
  public timespans_hash: ResponseData;
  public time_pix:       TimeVsPix;
  private _resource_tags: string[];

  resourceTags() {
    if (!this._resource_tags) {
      this._resource_tags =
        this.resource_specs.map(resource_spec => resource_spec.tag)
    }
    return this._resource_tags
  }

  kindFromTag(tag) {
    return tag.split('_')[0]
  }
}