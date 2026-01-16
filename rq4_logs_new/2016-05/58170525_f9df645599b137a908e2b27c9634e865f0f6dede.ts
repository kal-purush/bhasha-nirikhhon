import {Component, Input, Output, EventEmitter} from '@angular/core';
import {Container} from '../containers/container';
import {MemoryFormatPipe} from '../util/memoryFormat.pipe';
import {SortByIdPipe} from './sort-by-id.pipe';
import {MemoryAllocationPipe} from './memory-allocation.pipe';

@Component({
    selector: 'vmw-containers-list',
    templateUrl: 'app/solution/containers-list.html',
    pipes: [MemoryFormatPipe, SortByIdPipe, MemoryAllocationPipe],
    styleUrls: ['app/solution/containers-list.css']
})
export class ContainersListComponent {

  @Input()
  private containers: Container[];

  @Input()
  private memoryFilterThreshold: number = 0;

  @Input()
  private idOrder: boolean;

  @Output()
  stateChange: EventEmitter = new EventEmitter();

  notifyStateChange(c: Container) {
    this.stateChange.emit({ container: c });
  }

  highMemory(container: Container) {
    return container.utilization.memory > (container.memory * this.memoryFilterThreshold);
  }
}