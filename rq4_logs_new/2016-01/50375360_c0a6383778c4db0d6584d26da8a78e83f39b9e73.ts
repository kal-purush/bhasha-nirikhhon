import {Component} from 'angular2/core';
import {TimeVsPix} from './time-vs-pix';
import {UseBlockComponent}  from './use-block.component';
import {ResourceTimeBlock}  from './resource-time-block';

@Component({
  selector: 'timespan',
  // Invoke as: 
  //   <timespan [time_blocks]="[a_time_block]" [time_pix]="time_pix"/>
  template: `
    <div *ngFor="#time_block of time_blocks" class="timespan">
      <use-block [block]="time_block" [time_pix]="time_pix"></use-block>
    </div>
  `,
  styleUrls: ['app/scheduled-resource.css'],
  directives: [UseBlockComponent],
  inputs: ['time_blocks', 'time_pix'],
  providers: [] // TimeBlocksService ???
})

export class TimespanComponent {
  public time_blocks: ResourceTimeBlock[];
  public time_pix: TimeVsPix;
}