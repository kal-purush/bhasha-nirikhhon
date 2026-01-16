import {Component,EventEmitter,Input,Output} from 'angular2/core';
import {DateFromStringPipe} from '../utils/pipes/dateFromStringPipe';
import {FlightInterface} from '../model/flightInterface';

/**
 * Flight view
 */
@Component({
    selector: 'flight',
    templateUrl: 'html/flight-list-item.component.html',
    events: ['selected'],
    pipes: [DateFromStringPipe]
})
export class FlightListItemComponent{
    /** Model */
    @Input() model: FlightInterface;
    
    /** Event triggered on selection */
    @Output() selected = new EventEmitter<FlightInterface>();
    
    /** Selection trigger */
    onSelection() {
        this.selected.emit(this.model);
    }
}