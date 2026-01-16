import { customElement, useView } from 'aurelia-templating';
import { EventAggregator } from 'aurelia-event-aggregator';
import jquery from 'jquery';
import 'bootstrap';

@customElement('error-modal')
@useView('./error-modal.html')
export class ErrorModal {
	protected errorMessage: string;

	public constructor(private eventAggregator: EventAggregator) {
		this.eventAggregator.subscribe(Error, (error: Error) => this.show(error));
	}

	public show(errorMessage: string) {
		this.errorMessage = errorMessage;
		jquery('#error-modal-id').modal('show');
	}
}