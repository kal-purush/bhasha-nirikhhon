import { Component, Injector, ViewChild, TemplateRef } from '@angular/core';
import { ContentBlockBaseComponent } from './content-block-base.component';
import { IPageEvent } from '../common/models/page-event';
import { EventTypes } from '../common/models/event-types';
import { BsModalService } from 'ngx-bootstrap/modal';
import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';

@Component({
	moduleId: module.id,
	selector: 'app-confirmation-content-block',
	templateUrl: 'confirmation-content-block.component.html'
})
export class ConfirmationContentBlockComponent extends ContentBlockBaseComponent {
	@ViewChild('content') public contentRef: TemplateRef<any>;
	events: string[] = [];
	title: string;
	message: string;
	okButtonTitle: string;
	cancelButtonTitle: string;
	modalRef: BsModalRef;
	itemId: string;

	constructor(injector: Injector, private modalService: BsModalService) {
		super(injector);
	}

	handleEvent(event: IPageEvent) {
		if (event.componentTargets && event.componentTargets.find((target) => target === this.id)) {
			switch (event.eventType) {
				case EventTypes.GridDeleteClicked:
					this.title = event.data.title;
					this.message = event.data.message;
					this.okButtonTitle = event.data.okButtonTitle;
					this.cancelButtonTitle = event.data.cancelButtonTitle;
					this.itemId = event.data.itemId;

					this.modalRef = this.modalService.show(this.contentRef, {class: 'modal-md'});

					console.log('grid delete item clicked');
					break;
			}
		}
	}

	confirm(): void {
		const targets = this.eventRecipients ? this.eventRecipients : [''];
		this.emitEvent.next({
			name: 'delete confirmed',
			componentRaisedBy: this.id,
			componentTargets: targets,
			eventType: EventTypes.ConfirmationOkClicked,
			data: { itemId: this.itemId }
		});
		this.modalRef.hide();
	}

	decline(): void {
		const targets = this.eventRecipients ? this.eventRecipients : [''];
		this.emitEvent.next({
			name: 'delete cancelled',
			componentRaisedBy: this.id,
			componentTargets: targets,
			eventType: EventTypes.ConfirmationCancelClicked,
			data: { itemId: this.itemId }
		});
		this.modalRef.hide();
	}
}