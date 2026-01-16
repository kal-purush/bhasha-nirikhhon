import { Component, Injector, OnInit } from '@angular/core';
import { ContentBlockBaseComponent } from './content-block-base.component';
import { IPageEvent } from '../common/models/page-event';
import { ODataService } from '../services/o-data.service';
import { EventTypes } from '../common/models/event-types';

@Component({
	moduleId: module.id,
	selector: 'app-data-grid-content-block',
	templateUrl: 'data-grid-content-block.component.html'
})
export class DataGridContentBlockComponent extends ContentBlockBaseComponent implements OnInit {
	title = '';
	items: any[];
	dataUrl: string;
	objectName: string;

	constructor(injector: Injector, private oDataService: ODataService) {
		super(injector);
		this.title = injector.get('title');
		this.dataUrl = injector.get('dataUrl');
		this.objectName = injector.get('objectName');
	}

	ngOnInit(): void {
		if (this.dataUrl && this.objectName) {
			this.oDataService.getItems(this.dataUrl, this.objectName, false).subscribe((items) => {
				this.items = items;
			});
		}
	}

	gridItemDeleteClicked(itemId: string) {
		const targets = this.eventRecipients ? this.eventRecipients : [''];
		this.emitEvent.next({
			name: EventTypes.GridDeleteClicked.toString(),
			componentRaisedBy: this.id,
			componentTargets: targets,
			eventType: EventTypes.GridDeleteClicked,
			data: { itemId: itemId }
		});
	}

	gridRowClicked(itemId: string) {
		const targets = this.eventRecipients ? this.eventRecipients : [''];
		this.emitEvent.next({
			name: EventTypes.GridRowClicked.toString(),
			componentRaisedBy: this.id,
			componentTargets: targets,
			eventType: EventTypes.GridRowClicked,
			data: { itemId: itemId }
		});
	}

	gridNewItemClicked() {
		const targets = this.eventRecipients ? this.eventRecipients : [''];
		this.emitEvent.next({
			name: EventTypes.GridNewItemClicked.toString(),
			componentRaisedBy: this.id,
			componentTargets: targets,
			eventType: EventTypes.GridNewItemClicked,
			data: {  }
		});
	}

	handleEvent(event: IPageEvent) {
		if (event.componentTargets && event.componentTargets.find((target) => target === this.id)) {
			switch (event.eventType) {
				case EventTypes.ItemSaved:
					this.items[event.data.id] = event.data;
					console.log('item saved event');
					console.log('Event was for me ' + this.id + ' :-) - I must do something with this data');
					console.log(event.data);
					break;
			}
		}
	}
}