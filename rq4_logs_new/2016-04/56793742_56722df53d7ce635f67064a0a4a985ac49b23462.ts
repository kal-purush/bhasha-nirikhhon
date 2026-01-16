import {Component, Inject} from 'angular2/core'
import {RouteParams} from 'angular2/router'
import {TenderFraudComplaintService} from './TenderFraudComplaintService'

@Component({
    selector: 'search-tender-fraud-complaint',
    providers: [TenderFraudComplaintService],
    template: `
    <div *ngIf="tenderFraudComplaints">
    <h4 class="ui header information">Tender fraud complaints matching {{searchText}} </h4>
    <table class="ui celled table">
    <thead>
    <tr>
    <th>Fraud Complaint Details</th>
    <th>Offending Company</th>
    <th>Priority</th>
    <th>Reference</th>
    <th>Reported On</th>
    <th>Reported By</th>
    </tr>
</thead>
<tbody>
<tr *ngFor="#complaint of tenderFraudComplaints">
<td><div class="ui ribbon label">{{complaint.complaint}}</div></td>
<td><div class="ui label ribbon">{{complaint.offendingCompany}}</div></td>
<td><div class="ui label ribbon">{{complaint.complaintPriority}}</div></td>
<td><div class="ui ribbon label">{{complaint.reference}}</div></td>
<td><div class="ui label ribbon">{{complaint.reportedOn}}</div></td>
<td><div class="ui label ribbon">{{complaint.complainantName}}</div></td>
</tr>
</tbody>
</table>
</div>
<h4 *ngIf="!tenderFraudComplaints" class="ui header information">No tender fraud complaints found matching {{searchText}}.</h4>
`
})
export class SearchTenderFraudComplaintComponent {

    searchText:string;
    tenderFraudComplaints:Array<any>;

    constructor(@Inject(RouteParams) private routeParams:RouteParams, @Inject(TenderFraudComplaintService)service:TenderFraudComplaintService) {
        this.searchText = routeParams.get('searchText');
        console.log(`Got search string ${this.searchText} from the route param. Please wait...`);
        service.searchTenderFraudComplaints(this.searchText).subscribe(complaints=> {
            this.tenderFraudComplaints = complaints;
        });
    }
}